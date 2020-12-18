const std = @import("std");
const ThreadPool = @This();

lock: std.Mutex = .{},
is_running: bool = true,
allocator: *std.mem.Allocator,
threads: std.fifo.LinearFifo(*std.Thread, .Dynamic),
run_queue: std.fifo.LinearFifo(*Runnable, .Dynamic),
idle_queue: std.fifo.LinearFifo(*std.AutoResetEvent, .Dynamic),

const Runnable = struct {
    runFn: fn (*Runnable) void,
};

pub fn init(self: *ThreadPool, allocator: *std.mem.Allocator) !void {
    self.* = .{
        .allocator = allocator,
        .threads = @TypeOf(self.threads).init(allocator),
        .run_queue = @TypeOf(self.run_queue).init(allocator),
        .idle_queue = @TypeOf(self.idle_queue).init(allocator),
    };

    errdefer self.deinit();

    var num_threads = std.Thread.cpuCount() catch 1;
    while (num_threads > 0) : (num_threads -= 1) {
        const thread = try std.Thread.spawn(self, runWorker);
        try self.threads.writeItem(thread);
    }
}

pub fn deinit(self: *ThreadPool) void {
    self.shutdown();

    std.debug.assert(!self.is_running);
    while (self.threads.readItem()) |thread|
        thread.wait();

    self.threads.deinit();
    self.run_queue.deinit();
    self.idle_queue.deinit();
}

pub fn shutdown(self: *ThreadPool) void {
    const held = self.lock.acquire();
    defer held.release();

    self.is_running = false;
    while (self.idle_queue.readItem()) |event|
        event.set();
}

pub fn spawn(self: *ThreadPool, comptime func: anytype, args: anytype) !void {
    const Args = @TypeOf(args);
    const Closure = struct {
        arguments: Args,
        pool: *ThreadPool,
        runnable: Runnable = .{ .runFn = runFn },

        fn runFn(runnable: *Runnable) void {
            const closure = @fieldParentPtr(@This(), "runnable", runnable);
            const result = @call(.{}, func, closure.arguments);
            closure.pool.allocator.destroy(closure);
        }
    };

    const closure = try self.allocator.create(Closure);
    errdefer self.allocator.destroy(closure);
    closure.* = .{
        .arguments = args,
        .pool = self,
    };

    const held = self.lock.acquire();
    defer held.release();

    try self.run_queue.writeItem(&closure.runnable);
    if (self.idle_queue.readItem()) |event|
        event.set();
}

fn runWorker(self: *ThreadPool) void {
    while (true) {
        const held = self.lock.acquire();

        if (self.run_queue.readItem()) |runnable| {
            held.release();
            (runnable.runFn)(runnable);
            continue;
        }

        if (!self.is_running) {
            held.release();
            return;
        }

        var event = std.AutoResetEvent{};
        self.idle_queue.writeItem(&event) catch return held.release();
        held.release();
        event.wait();
    }
}
