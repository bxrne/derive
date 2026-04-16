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

    // Demo executable: contiguous index
    const demo_contiguous_mod = b.createModule(.{
        .root_source_file = b.path("demo/contiguous/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "libderive", .module = libderive },
        },
    });

    const demo_contiguous_exe = b.addExecutable(.{
        .name = "demo-contiguous",
        .root_module = demo_contiguous_mod,
    });
    b.installArtifact(demo_contiguous_exe);

    const demo_contiguous_run = b.addRunArtifact(demo_contiguous_exe);
    demo_contiguous_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        demo_contiguous_run.addArgs(args);
    }

    const demo_contiguous_step = b.step("demo-contiguous", "Build and run the contiguous demo executable");
    demo_contiguous_step.dependOn(&demo_contiguous_run.step);

    // Demo executable: treap index
    const demo_tree_mod = b.createModule(.{
        .root_source_file = b.path("demo/tree/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "libderive", .module = libderive },
        },
    });

    const demo_tree_exe = b.addExecutable(.{
        .name = "demo-tree",
        .root_module = demo_tree_mod,
    });
    b.installArtifact(demo_tree_exe);

    const demo_tree_run = b.addRunArtifact(demo_tree_exe);
    demo_tree_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        demo_tree_run.addArgs(args);
    }

    const demo_tree_step = b.step("demo-tree", "Build and run the treap demo executable");
    demo_tree_step.dependOn(&demo_tree_run.step);

    // Tests for libderive, demo, and dst
    const libderive_tests = b.addTest(.{
        .root_module = libderive,
    });
    const run_libderive_tests = b.addRunArtifact(libderive_tests);

    const demo_contiguous_tests = b.addTest(.{
        .root_module = demo_contiguous_mod,
    });
    const run_demo_contiguous_tests = b.addRunArtifact(demo_contiguous_tests);

    const demo_tree_tests = b.addTest(.{
        .root_module = demo_tree_mod,
    });
    const run_demo_tree_tests = b.addRunArtifact(demo_tree_tests);

    const test_step = b.step("test", "Run libderive and demo tests");
    test_step.dependOn(&run_libderive_tests.step);
    test_step.dependOn(&run_demo_contiguous_tests.step);
    test_step.dependOn(&run_demo_tree_tests.step);
}
