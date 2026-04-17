const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const libderive = b.addModule("libderive", .{
        .root_source_file = b.path("libderive/mod.zig"),
        .target = target,
        .optimize = optimize,
    });

    const libderive_lib = b.addLibrary(.{
        .name = "libderive",
        .linkage = .static,
        .root_module = libderive,
    });
    b.installArtifact(libderive_lib);

    const install_docs = b.addInstallDirectory(.{
        .source_dir = libderive_lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "../docs/libderive",
    });
    b.step("docs", "Generate and install documentation").dependOn(&install_docs.step);

    const lubm = b.createModule(.{
        .root_source_file = b.path("demo/lubm.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "libderive", .module = libderive }},
    });

    addDemo(b, libderive, lubm, "demo-contiguous", "demo/contiguous/main.zig", "Build and run the contiguous demo executable");
    addDemo(b, libderive, lubm, "demo-tree", "demo/tree/main.zig", "Build and run the treap demo executable");

    const libderive_tests = b.addTest(.{ .root_module = libderive });
    const run_libderive_tests = b.addRunArtifact(libderive_tests);
    b.step("test", "Run libderive tests").dependOn(&run_libderive_tests.step);
}

/// Wire up a demo executable with install + named run step.
fn addDemo(
    b: *std.Build,
    libderive: *std.Build.Module,
    lubm: *std.Build.Module,
    comptime name: []const u8,
    comptime root: []const u8,
    comptime description: []const u8,
) void {
    const module = b.createModule(.{
        .root_source_file = b.path(root),
        .target = libderive.resolved_target.?,
        .optimize = libderive.optimize.?,
        .imports = &.{
            .{ .name = "libderive", .module = libderive },
            .{ .name = "lubm", .module = lubm },
        },
    });
    const exe = b.addExecutable(.{ .name = name, .root_module = module });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    b.step(name, description).dependOn(&run.step);
}
