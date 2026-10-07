const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const amqp_mod = b.addModule("amqp", .{
        .root_source_file = b.path("src/amqp.zig"),
        .target = target,
        .optimize = optimize,
    });

    const main_tests = b.addTest(.{
        .root_module = amqp_mod,
    });

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run library unit tests");
    test_step.dependOn(&run_main_tests.step);

    const check_step = b.step("check", "Check compilation without running tests");
    check_step.dependOn(&main_tests.step);

    // Examples
    const build_examples = b.option(bool, "examples", "Build and install example executables") orelse true;

    const examples = [_][]const u8{ "publisher", "consumer", "confirms" };
    inline for (examples) |name| {
        const exe_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("examples/{s}.zig", .{name})),
            .target = target,
            .optimize = optimize,
        });
        exe_mod.addImport("amqp", amqp_mod);

        const exe = b.addExecutable(.{
            .name = name,
            .root_module = exe_mod,
        });
        if (build_examples) {
            b.installArtifact(exe);
        }

        const run_cmd = b.addRunArtifact(exe);
        const run_step = b.step(b.fmt("run-{s}", .{name}), b.fmt("Run the {s} example", .{name}));
        run_step.dependOn(&run_cmd.step);
    }
}
