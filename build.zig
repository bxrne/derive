const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library module for libderive
    const libderive = b.addModule("libderive", .{
        .root_source_file = b.path("libderive/mod.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Library artifact for libderive
    const libderive_lib = b.addLibrary(.{
        .name = "libderive",
        .linkage = .static,
        .root_module = libderive,
    });

    // Install libderive and its documentation
    const install_docs = b.addInstallDirectory(.{
        .source_dir = libderive_lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "../docs/libderive",
    });

    const docs_step = b.step("docs", "Generate and install documentation");
    docs_step.dependOn(&install_docs.step);
    b.installArtifact(libderive_lib);

    // Demo executable
    const demo_mod = b.createModule(.{
        .root_source_file = b.path("demo/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "libderive", .module = libderive },
        },
    });

    const demo_exe = b.addExecutable(.{
        .name = "demo",
        .root_module = demo_mod,
    });
    b.installArtifact(demo_exe);

    const demo_run = b.addRunArtifact(demo_exe);
    demo_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        demo_run.addArgs(args);
    }

    const demo_step = b.step("demo", "Build and run the demo executable");
    demo_step.dependOn(&demo_run.step);

    // Tests for libderive, demo, and dst
    const libderive_tests = b.addTest(.{
        .root_module = libderive,
    });
    const run_libderive_tests = b.addRunArtifact(libderive_tests);

    const demo_tests = b.addTest(.{
        .root_module = demo_mod,
    });
    const run_demo_tests = b.addRunArtifact(demo_tests);

    const test_step = b.step("test", "Run libderive, demo, and dst tests");
    test_step.dependOn(&run_libderive_tests.step);
    test_step.dependOn(&run_demo_tests.step);
}
