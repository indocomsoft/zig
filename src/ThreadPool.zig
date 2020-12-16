const std = @import("std");
const ThreadPool = @This();

allocator: *std.mem.Allocator,
mutex: std.Mutex = std.Mutex{},
threads: []*std.Thread,
is_shutdown: bool = false,
run_queue: RunQueue = RunQueue{},
idle_queue: IdleQueue = IdleQueue{},

threadlocal var tls_current_pool: ?*ThreadPool = null;

pub fn get() ?*ThreadPool {
    return tls_current_pool;
}

const IdleQueue = std.SinglyLinkedList(std.AutoResetEvent);
const RunQueue = struct {
    head: ?*Runnable = null,
    tail: ?*Runnable = null,

    fn push(self: *RunQueue, runnable: *Runnable) void {
        if (self.tail) |tail|
            tail.next = runnable;
        if (self.head == null)
            self.head = runnable;
        self.tail = runnable;
        runnable.next = null;
    }

    fn pop(self: *RunQueue) ?*Runnable {
        const runnable = self.head orelse return null;
        self.head = runnable.next;
        if (self.head == null) self.tail = null;
        return runnable;
    }
};

pub const Callback = fn (*Runnable) void;
pub const Runnable = struct {
    next: ?*Runnable = undefined,
    callback: Callback,

    pub fn init(callback: Callback) Runnable {
        return Runnable{ .callback = callback };
    }
};

pub const Options = struct {
    allocator: ?*std.mem.Allocator = null,
    max_threads: ?usize = null,
};

fn ReturnTypeOf(comptime entryFn: anytype) type {
    return @typeInfo(@TypeOf(entryFn)).Fn.return_type.?;
}

pub fn run(options: Options, comptime entryFn: anytype, args: anytype) !ReturnTypeOf(entryFn) {
    const Args = @TypeOf(args);
    const Result = ReturnTypeOf(entryFn);
    const Wrapper = struct {
        fn_args: Args,
        runnable: Runnable,
        result: Result,

        fn callback(runnable: *Runnable) void {
            const self = @fieldParentPtr(@This(), "runnable", runnable);
            self.result = @call(.{}, entryFn, self.fn_args);
            ThreadPool.get().?.shutdown();
        }
    };

    var wrapper: Wrapper = undefined;
    wrapper.fn_args = args;
    wrapper.runnable = Runnable{ .callback = Wrapper.callback };

    const allocator = options.allocator orelse std.heap.page_allocator;
    const extra_threads =
        if (std.builtin.single_threaded) @as(usize, 0) else if (options.max_threads) |t| std.math.max(1, t) - 1 else std.math.max(1, std.Thread.cpuCount() catch 1) - 1;

    try runWithThreads(allocator, extra_threads, &wrapper.runnable);
    return wrapper.result;
}

fn runWithThreads(allocator: *std.mem.Allocator, extra_threads: usize, runnable: *Runnable) !void {
    const threads = if (extra_threads == 0) &[_]*std.Thread{} else try allocator.alloc(*std.Thread, extra_threads);
    defer allocator.free(threads);

    var self = ThreadPool{
        .threads = threads,
        .allocator = allocator,
    };
    self.run_queue.push(runnable);

    var spawned: usize = 0;
    defer for (self.threads[0..spawned]) |thread|
        thread.wait();

    for (self.threads) |*thread| {
        thread.* = std.Thread.spawn(&self, runWorker) catch break;
        spawned += 1;
    }

    self.runWorker();
}

fn runWorker(self: *ThreadPool) void {
    const old_pool = tls_current_pool;
    tls_current_pool = self;
    defer tls_current_pool = old_pool;

    while (true) {
        const held = self.mutex.acquire();
        if (self.is_shutdown) {
            held.release();
            break;
        }

        if (self.run_queue.pop()) |runnable| {
            held.release();
            (runnable.callback)(runnable);
            continue;
        }

        var idle_node = IdleQueue.Node{ .data = .{} };
        self.idle_queue.prepend(&idle_node);
        held.release();
        idle_node.data.wait();
    }
}

pub fn spawn(self: *ThreadPool, comptime runFn: anytype, args: anytype) !void {
    const Args = @TypeOf(args);
    const Wrapper = struct {
        fn_args: Args,
        allocator: *std.mem.Allocator,
        runnable: Runnable,

        fn run(runnable: *Runnable) void {
            const this = @fieldParentPtr(@This(), "runnable", runnable);
            const x = @call(.{}, runFn, this.fn_args);
            this.allocator.destroy(this);
        }
    };

    var wrapper = try self.allocator.create(Wrapper);
    wrapper.* = Wrapper{
        .fn_args = args,
        .allocator = self.allocator,
        .runnable = Runnable.init(Wrapper.run),
    };
    self.schedule(&wrapper.runnable);
}

pub fn schedule(self: *ThreadPool, runnable: *Runnable) void {
    const idle_node = blk: {
        const held = self.mutex.acquire();
        defer held.release();

        if (self.is_shutdown)
            return;

        self.run_queue.push(runnable);
        break :blk self.idle_queue.popFirst();
    };

    if (idle_node) |node|
        node.data.set();
}

pub fn shutdown(self: *ThreadPool) void {
    var idle_nodes = blk: {
        const held = self.mutex.acquire();
        defer held.release();

        if (self.is_shutdown)
            return;

        const nodes = self.idle_queue;
        self.idle_queue = IdleQueue{};
        self.is_shutdown = true;
        break :blk nodes;
    };

    while (idle_nodes.popFirst()) |node|
        node.data.set();
}
