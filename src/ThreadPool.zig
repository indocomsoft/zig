const std = @import("std");
const ThreadPool = @This();

lock: std.Mutex = .{},
running: usize = 0,
spawned: usize = 0,
spawnable: usize,
run_queue: Batch = .{},
idle_queue: ?*Worker = null,
spawned_queue: ?*Worker = null,

pub const Config = struct {
    max_threads: ?usize = null,
};

pub fn init(config: Config) ThreadPool {
    const max_threads = if (std.builtin.single_threaded)
        @as(usize, 1)
    else if (config.max_threads) |max_threads|
        std.math.max(1, max_threads)
    else
        std.math.max(1, std.Thread.cpuCount() catch 1);
    return .{ .spawnable = max_threads };
}

pub fn run(self: *ThreadPool) void {
    self.spawned = 1;
    Worker.run(null, self);
}

pub const SpawnConfig = struct {
    allocator: *std.mem.Allocator,
    hints: ScheduleHints = .{},
};

pub fn spawn(self: *ThreadPool, config: SpawnConfig, comptime func: anytype, args: anytype) !void {
    const Args = @TypeOf(args);
    const Closure = struct {
        fn_args: Args,
        runnable: Runnable,
        allocator: *std.mem.Allocator,

        fn callback(runnable: *Runnable) void {
            const this = @fieldParentPtr(@This(), "runnable", runnable);
            const result = @call(.{}, func, this.fn_args);
            this.allocator.destroy(this);
        }
    };

    const allocator = config.allocator;
    const closure = try allocator.create(Closure);
    closure.* = .{
        .fn_args = args,
        .runnable = Runnable.init(Closure.callback),
        .allocator = allocator,
    };

    const hints = config.hints;
    self.schedule(hints, &closure.runnable);
}

pub const ScheduleHints = struct {
    fifo: bool = false,
};

pub fn schedule(self: *ThreadPool, hints: ScheduleHints, batchable: anytype) void {
    const batch = Batch.from(batchable);
    if (batch.isEmpty())
        return;

    const held = self.lock.acquire();

    if (hints.fifo) {
        self.run_queue.pushBack(batch);
    } else {
        self.run_queue.pushFront(batch);
    }

    if (self.running > 0) {
        if (self.idle_queue) |idle_queue| {
            const worker = idle_queue;
            self.idle_queue = worker.idle_next;
            held.release();
            return worker.event.set();
        }

        if (!std.builtin.single_threaded and self.spawned < self.spawnable) {
            if (Worker.spawn(self))
                self.spawned += 1;
        }
    }

    held.release();
}

const Worker = struct {
    thread: ?*std.Thread,
    idle_next: ?*Worker = null,
    spawned_next: ?*Worker = null,
    event: std.AutoResetEvent = .{},

    fn spawn(pool: *ThreadPool) bool {
        const Spawner = struct {
            tpool: *ThreadPool,
            thread: *std.Thread = undefined,
            data_event: std.AutoResetEvent = .{},
            done_event: std.AutoResetEvent = .{},

            fn run(self: *@This()) void {
                self.data_event.wait();
                const tpool = self.tpool;
                const thread = self.thread;

                self.done_event.set();
                return Worker.run(thread, tpool);
            }
        };

        var spawner: Spawner = .{ .tpool = pool };
        spawner.thread = std.Thread.spawn(&spawner, Spawner.run) catch return false;
        spawner.data_event.set();
        spawner.done_event.wait();
        return true;
    }

    fn run(thread: ?*std.Thread, pool: *ThreadPool) void {
        var self: Worker = .{ .thread = thread };
        var held = pool.lock.acquire();

        self.spawned_next = pool.spawned_queue;
        pool.spawned_queue = &self;

        while (true) {
            if (pool.run_queue.popFront()) |runnable| {
                pool.running += 1;
                held.release();

                runnable.run();
                held = pool.lock.acquire();
                pool.running -= 1;
                continue;
            }

            if (pool.running > 0) {
                self.idle_next = pool.idle_queue;
                pool.idle_queue = &self;
                held.release();

                self.event.wait();
                held = pool.lock.acquire();
                continue;
            }

            pool.spawned -= 1;
            const is_last = pool.spawned == 0;
            const is_root_worker = thread == null;

            var idle_queue = pool.idle_queue;
            pool.idle_queue = null;
            held.release();

            while (true) {
                const idle_worker = idle_queue orelse break;
                idle_queue = idle_worker.idle_next;
                idle_worker.event.set();
            }

            if (is_last and !is_root_worker) {
                var root_worker = pool.spawned_queue orelse unreachable;
                while (root_worker.spawned_next) |next|
                    root_worker = next;
                root_worker.event.set();
            }

            if (!is_root_worker or !is_last)
                self.event.wait();

            if (is_root_worker) {
                while (true) {
                    const worker = pool.spawned_queue orelse break;
                    pool.spawned_queue = worker.spawned_next;
                    const idle_thread = worker.thread orelse continue;
                    worker.event.set();
                    idle_thread.wait();
                }
            }

            return;
        }
    }
};

pub const Batch = struct {
    head: ?*Runnable = null,
    tail: *Runnable = undefined,

    pub fn from(batchable: anytype) Batch {
        return switch (@TypeOf(batchable)) {
            Batch => return batchable,
            ?*Runnable => return from(batchable orelse return .{}),
            *Runnable => {
                batchable.next = null;
                return .{
                    .head = batchable,
                    .tail = batchable,
                };
            },
            else => unreachable,
        };
    }

    pub fn isEmpty(self: Batch) bool {
        return self.head == null;
    }

    pub fn pushFront(self: *Batch, batchable: anytype) void {
        const batch = from(batchable);
        if (batch.isEmpty())
            return;

        if (self.isEmpty()) {
            self.* = batch;
        } else {
            batch.tail.next = self.head;
            self.head = batch.head;
        }
    }

    pub fn pushBack(self: *Batch, batchable: anytype) void {
        const batch = from(batchable);
        if (batch.isEmpty())
            return;

        if (self.isEmpty()) {
            self.* = batch;
        } else {
            self.tail.next = batch.head;
            self.tail = batch.tail;
        }
    }

    pub fn popFront(self: *Batch) ?*Runnable {
        const runnable = self.head orelse return null;
        self.head = runnable.next;
        return runnable;
    }
};

pub const Runnable = struct {
    next: ?*Runnable = null,
    callback: Callback,

    pub fn init(callback: Callback) Runnable {
        return .{ .callback = callback };
    }

    pub fn run(self: *Runnable) void {
        return (self.callback)(self);
    }
};

pub const Callback = fn (
    *Runnable,
) void;
