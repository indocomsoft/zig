const std = @import("std");
const Thread = std.Thread;
const ThreadPool = @This();
const ResetEvent = std.ResetEvent;
const Allocator = std.mem.Allocator;

workers: []Worker,
/// Protects the other fields.
lock: std.Mutex = .{},
active_tasks: usize = 0,

const Worker = struct {
    pool: *ThreadPool,
    thread: *Thread,
    reset_event: ResetEvent,
    func: fn (context: usize) void,
    is_idle: bool,
    shutdown: bool = false,
    context: usize,
};

pub fn create(gpa: *Allocator) !*ThreadPool {
    const pool = try gpa.create(ThreadPool);
    errdefer gpa.destroy(pool);

    const core_count = try Thread.cpuCount();
    if (core_count == 1) {
        pool.* = .{
            .workers = &[0]Worker{},
        };
        return pool;
    }
    const worker_count = core_count - 1;
    const workers = try gpa.alloc(Worker, worker_count);
    errdefer gpa.free(workers);

    pool.* = .{
        .workers = workers,
    };

    // TODO handle if any of the spawns fail below
    for (workers) |*worker| {
        worker.* = .{
            .func = undefined,
            .context = undefined,
            .pool = pool,
            .reset_event = ResetEvent.init(),
            .is_idle = true,
            .thread = try Thread.spawn(worker, workerRun),
        };
    }

    return pool;
}

pub fn destroy(pool: *ThreadPool, allocator: *Allocator) void {
    {
        var lock = pool.lock.acquire();
        defer lock.release();

        for (pool.workers) |*worker| {
            worker.shutdown = true;
            worker.reset_event.set();
        }
    }

    for (pool.workers) |worker| {
        worker.thread.wait();
    }

    allocator.free(pool.workers);
    allocator.destroy(pool);
}

/// Must be called from the main thread.
pub fn run(pool: *ThreadPool, context: anytype, comptime callback: fn (@TypeOf(context)) void) void {
    if (pool.workers.len == 0) {
        return callback(context);
    }
    const Context = @TypeOf(context);
    const S = struct {
        fn untyped_callback(data: usize) void {
            callback(@intToPtr(Context, data));
        }
    };
    const idle_worker: ?*Worker = blk: {
        var lock = pool.lock.acquire();
        defer lock.release();

        for (pool.workers) |*worker| {
            if (worker.is_idle) {
                pool.active_tasks += 1;
                break :blk worker;
            }
        } else {
            break :blk null;
        }
    };
    if (idle_worker) |w| {
        w.is_idle = false;
        w.context = @ptrToInt(context);
        w.func = S.untyped_callback;
        w.reset_event.set(); // wakeup, sleepy head
    } else {
        return callback(context);
    }
}

fn workerRun(worker: *Worker) void {
    while (true) {
        worker.reset_event.wait();
        if (worker.shutdown) return;
        worker.func(worker.context);
        worker.reset_event.reset();
        {
            var lock = worker.pool.lock.acquire();
            defer lock.release();
            worker.is_idle = true;
            worker.pool.active_tasks -= 1;
        }
    }
}

pub fn finishActiveTasks(pool: *ThreadPool) void {
    const any_active_tasks = blk: {
        var lock = pool.lock.acquire();
        defer lock.release();

        for (pool.workers) |*worker| {
            if (!worker.is_idle) {
                break :blk true;
            }
        } else {
            break :blk false;
        }
    };
}

test "basic usage" {
    var pool = try ThreadPool.create(std.testing.allocator);
    var ctx: TestContext = .{
        .input1_a = 11,
        .input1_b = 22,
        .result1 = undefined,

        .input2_a = 10,
        .input2_b = 11,
        .result2 = undefined,
    };
    {
        defer pool.destroy(std.testing.allocator);
        defer pool.finishActiveTasks();

        pool.run(&ctx, work1);
        pool.run(&ctx, work2);
    }
    std.testing.expectEqual(@as(i32, 33), ctx.result1);
    std.testing.expectEqual(@as(i32, 110), ctx.result2);
}

const TestContext = struct {
    input1_a: i32,
    input1_b: i32,
    result1: i32,

    input2_a: i32,
    input2_b: i32,
    result2: i32,
};

fn work1(context: *TestContext) void {
    std.debug.print("1 context: {}\n", .{context.*});
    context.result1 = context.input1_a + context.input1_b;
}

fn work2(context: *TestContext) void {
    std.debug.print("2 context: {}\n", .{context.*});
    context.result2 = context.input2_a * context.input2_b;
}
