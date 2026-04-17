//! Shared LUBM-style RDF generator and benchmark runner for the two
//! backend demos. Emits a Lehigh University Benchmark-shaped dataset
//! (universities → departments → faculty, students, courses, publications)
//! using real vocabularies, then times the canonical basic-graph-patterns.
//!
//! Timings are taken only over the live in-memory dataset; WAL durability
//! is exercised in a separate, untimed section so load measurements are
//! not distorted by journal IO.

const std = @import("std");
const Io = std.Io;
const libderive = @import("libderive");
const RDFDataset = libderive.RDFDataset;
const Input = libderive.Input;

/// Take a monotonic (`awake`) timestamp. Thin wrapper so call sites stay one line.
fn nowMonotonic(io: Io) Io.Timestamp {
    return Io.Timestamp.now(io, .awake);
}

/// Nanoseconds between two monotonic timestamps, saturating at u64.
fn elapsedNanos(start: Io.Timestamp, end: Io.Timestamp) u64 {
    const delta = start.durationTo(end).toNanoseconds();
    return if (delta < 0) 0 else @intCast(delta);
}

pub const ns_ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#";
pub const ns_inst = "urn:derive:lubm:";
pub const rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub const rdfs_label = "http://www.w3.org/2000/01/rdf-schema#label";
pub const graph_default = ns_inst ++ "graph";

pub const type_university = ns_ub ++ "University";
pub const type_department = ns_ub ++ "Department";
pub const type_full_professor = ns_ub ++ "FullProfessor";
pub const type_associate_professor = ns_ub ++ "AssociateProfessor";
pub const type_lecturer = ns_ub ++ "Lecturer";
pub const type_graduate_student = ns_ub ++ "GraduateStudent";
pub const type_undergraduate_student = ns_ub ++ "UndergraduateStudent";
pub const type_course = ns_ub ++ "Course";
pub const type_publication = ns_ub ++ "Publication";

pub const pred_sub_organization_of = ns_ub ++ "subOrganizationOf";
pub const pred_works_for = ns_ub ++ "worksFor";
pub const pred_member_of = ns_ub ++ "memberOf";
pub const pred_teacher_of = ns_ub ++ "teacherOf";
pub const pred_takes_course = ns_ub ++ "takesCourse";
pub const pred_advisor = ns_ub ++ "advisor";
pub const pred_publication_author = ns_ub ++ "publicationAuthor";
pub const pred_name = ns_ub ++ "name";
pub const pred_email = ns_ub ++ "emailAddress";

/// Per-university fan-out. Chosen so one university emits ~8.5k quads.
const per_university = struct {
    const departments: u32 = 10;
    const full_professors: u32 = 5;
    const associate_professors: u32 = 10;
    const lecturers: u32 = 5;
    const graduate_students: u32 = 20;
    const undergraduates: u32 = 50;
    const courses: u32 = 20;
    const publications: u32 = 30;
    const courses_per_student: u32 = 4;
    const authors_per_publication: u32 = 2;
};

/// Reusable IRI formatting buffer. LUBM instance IRIs are short; 96 bytes
/// is far above the longest path we generate (`urn:derive:lubm:u99/d9/pub29`).
const IriBuffer = [96]u8;

fn formatIri(buffer: *IriBuffer, comptime fmt: []const u8, args: anytype) []const u8 {
    return std.fmt.bufPrint(buffer, ns_inst ++ fmt, args) catch unreachable;
}

/// Summary of a load pass: how many quads were inserted and how long it took.
pub const LoadStats = struct {
    target: u64,
    inserted: u64,
    universities: u32,
    elapsed_ns: u64,
};

/// Generate and insert LUBM-shaped quads until at least `target_quads` have
/// been added. Returns insert count and elapsed time for just the load loop.
pub fn load(dataset: *RDFDataset, io: Io, target_quads: u64) !LoadStats {
    var subject_buffer: IriBuffer = undefined;
    var object_buffer: IriBuffer = undefined;
    var literal_buffer: [128]u8 = undefined;

    const start = nowMonotonic(io);
    var university_index: u32 = 0;
    while (dataset.statementCount() < target_quads) : (university_index += 1) {
        try emitUniversity(dataset, university_index, &subject_buffer, &object_buffer, &literal_buffer);
    }
    const elapsed = elapsedNanos(start, nowMonotonic(io));

    return .{
        .target = target_quads,
        .inserted = dataset.statementCount(),
        .universities = university_index,
        .elapsed_ns = elapsed,
    };
}

fn emitUniversity(
    dataset: *RDFDataset,
    university: u32,
    subject_buffer: *IriBuffer,
    object_buffer: *IriBuffer,
    literal_buffer: *[128]u8,
) !void {
    const uni_iri = formatIri(subject_buffer, "u{d}", .{university});
    try dataset.addQuad(.{ .iri = uni_iri }, rdf_type, .{ .iri = type_university }, graph_default);

    const uni_label = try std.fmt.bufPrint(literal_buffer, "University {d}", .{university});
    try dataset.addQuad(.{ .iri = uni_iri }, rdfs_label, .{ .literal = .{ .value = uni_label, .lang = "en" } }, graph_default);

    var department: u32 = 0;
    while (department < per_university.departments) : (department += 1) {
        try emitDepartment(dataset, university, department, subject_buffer, object_buffer, literal_buffer);
    }
}

fn emitDepartment(
    dataset: *RDFDataset,
    university: u32,
    department: u32,
    subject_buffer: *IriBuffer,
    object_buffer: *IriBuffer,
    literal_buffer: *[128]u8,
) !void {
    var dept_buffer: IriBuffer = undefined;
    const dept_iri = formatIri(&dept_buffer, "u{d}/d{d}", .{ university, department });
    const uni_iri = formatIri(object_buffer, "u{d}", .{university});

    try dataset.addQuad(.{ .iri = dept_iri }, rdf_type, .{ .iri = type_department }, graph_default);
    try dataset.addQuad(.{ .iri = dept_iri }, pred_sub_organization_of, .{ .iri = uni_iri }, graph_default);

    try emitFaculty(dataset, dept_iri, university, department, type_full_professor, "prof", 0, per_university.full_professors, subject_buffer, object_buffer, literal_buffer);
    try emitFaculty(dataset, dept_iri, university, department, type_associate_professor, "assoc", per_university.full_professors, per_university.associate_professors, subject_buffer, object_buffer, literal_buffer);
    try emitFaculty(dataset, dept_iri, university, department, type_lecturer, "lect", per_university.full_professors + per_university.associate_professors, per_university.lecturers, subject_buffer, object_buffer, literal_buffer);

    try emitCourses(dataset, dept_iri, university, department, subject_buffer);
    try emitStudents(dataset, dept_iri, university, department, subject_buffer, object_buffer, literal_buffer);
    try emitPublications(dataset, university, department, subject_buffer, object_buffer);
}

fn emitFaculty(
    dataset: *RDFDataset,
    dept_iri: []const u8,
    university: u32,
    department: u32,
    class_iri: []const u8,
    role: []const u8,
    offset: u32,
    count: u32,
    subject_buffer: *IriBuffer,
    object_buffer: *IriBuffer,
    literal_buffer: *[128]u8,
) !void {
    var index: u32 = 0;
    while (index < count) : (index += 1) {
        const person_iri = formatIri(subject_buffer, "u{d}/d{d}/{s}{d}", .{ university, department, role, index });
        try dataset.addQuad(.{ .iri = person_iri }, rdf_type, .{ .iri = class_iri }, graph_default);
        try dataset.addQuad(.{ .iri = person_iri }, pred_works_for, .{ .iri = dept_iri }, graph_default);

        const name = try std.fmt.bufPrint(literal_buffer[0..64], "{s} {d}-{d}", .{ role, department, offset + index });
        try dataset.addQuad(.{ .iri = person_iri }, pred_name, .{ .literal = .{ .value = name } }, graph_default);

        const email = try std.fmt.bufPrint(literal_buffer[64..128], "{s}{d}@u{d}.example.org", .{ role, offset + index, university });
        try dataset.addQuad(.{ .iri = person_iri }, pred_email, .{ .literal = .{ .value = email } }, graph_default);

        const teaches = formatIri(object_buffer, "u{d}/d{d}/course{d}", .{ university, department, index % per_university.courses });
        try dataset.addQuad(.{ .iri = person_iri }, pred_teacher_of, .{ .iri = teaches }, graph_default);
    }
}

fn emitCourses(
    dataset: *RDFDataset,
    dept_iri: []const u8,
    university: u32,
    department: u32,
    subject_buffer: *IriBuffer,
) !void {
    var course: u32 = 0;
    while (course < per_university.courses) : (course += 1) {
        const course_iri = formatIri(subject_buffer, "u{d}/d{d}/course{d}", .{ university, department, course });
        try dataset.addQuad(.{ .iri = course_iri }, rdf_type, .{ .iri = type_course }, graph_default);
        try dataset.addQuad(.{ .iri = course_iri }, pred_sub_organization_of, .{ .iri = dept_iri }, graph_default);
    }
}

fn emitStudents(
    dataset: *RDFDataset,
    dept_iri: []const u8,
    university: u32,
    department: u32,
    subject_buffer: *IriBuffer,
    object_buffer: *IriBuffer,
    literal_buffer: *[128]u8,
) !void {
    var grad: u32 = 0;
    while (grad < per_university.graduate_students) : (grad += 1) {
        const student_iri = formatIri(subject_buffer, "u{d}/d{d}/grad{d}", .{ university, department, grad });
        try dataset.addQuad(.{ .iri = student_iri }, rdf_type, .{ .iri = type_graduate_student }, graph_default);
        try dataset.addQuad(.{ .iri = student_iri }, pred_member_of, .{ .iri = dept_iri }, graph_default);

        const advisor_iri = formatIri(object_buffer, "u{d}/d{d}/prof{d}", .{ university, department, grad % per_university.full_professors });
        try dataset.addQuad(.{ .iri = student_iri }, pred_advisor, .{ .iri = advisor_iri }, graph_default);

        const name = try std.fmt.bufPrint(literal_buffer, "grad {d}-{d}", .{ department, grad });
        try dataset.addQuad(.{ .iri = student_iri }, pred_name, .{ .literal = .{ .value = name } }, graph_default);

        var course: u32 = 0;
        while (course < per_university.courses_per_student) : (course += 1) {
            const course_iri = formatIri(object_buffer, "u{d}/d{d}/course{d}", .{ university, department, (grad + course) % per_university.courses });
            try dataset.addQuad(.{ .iri = student_iri }, pred_takes_course, .{ .iri = course_iri }, graph_default);
        }
    }

    var undergrad: u32 = 0;
    while (undergrad < per_university.undergraduates) : (undergrad += 1) {
        const student_iri = formatIri(subject_buffer, "u{d}/d{d}/ugrad{d}", .{ university, department, undergrad });
        try dataset.addQuad(.{ .iri = student_iri }, rdf_type, .{ .iri = type_undergraduate_student }, graph_default);
        try dataset.addQuad(.{ .iri = student_iri }, pred_member_of, .{ .iri = dept_iri }, graph_default);

        var course: u32 = 0;
        while (course < per_university.courses_per_student) : (course += 1) {
            const course_iri = formatIri(object_buffer, "u{d}/d{d}/course{d}", .{ university, department, (undergrad + course) % per_university.courses });
            try dataset.addQuad(.{ .iri = student_iri }, pred_takes_course, .{ .iri = course_iri }, graph_default);
        }
    }
}

fn emitPublications(
    dataset: *RDFDataset,
    university: u32,
    department: u32,
    subject_buffer: *IriBuffer,
    object_buffer: *IriBuffer,
) !void {
    var publication: u32 = 0;
    while (publication < per_university.publications) : (publication += 1) {
        const pub_iri = formatIri(subject_buffer, "u{d}/d{d}/pub{d}", .{ university, department, publication });
        try dataset.addQuad(.{ .iri = pub_iri }, rdf_type, .{ .iri = type_publication }, graph_default);

        var author: u32 = 0;
        while (author < per_university.authors_per_publication) : (author += 1) {
            const author_iri = formatIri(object_buffer, "u{d}/d{d}/prof{d}", .{ university, department, (publication + author) % per_university.full_professors });
            try dataset.addQuad(.{ .iri = pub_iri }, pred_publication_author, .{ .iri = author_iri }, graph_default);
        }
    }
}

/// Labeled query result for the benchmark table.
pub const QueryResult = struct { label: []const u8, matches: u64, elapsed_ns: u64 };

/// Run the canonical benchmark query suite on a preloaded dataset.
/// Each query exercises a different scan-plan shape so the prefix chooser
/// and index backing can be observed independently.
pub fn runQuerySuite(dataset: *const RDFDataset, io: Io, out: *[6]QueryResult) void {
    out[0] = timeQuery(dataset, io, "type=FullProfessor", .{ .p = rdf_type, .o = .{ .iri = type_full_professor } });
    out[1] = timeQuery(dataset, io, "type=UndergraduateStudent", .{ .p = rdf_type, .o = .{ .iri = type_undergraduate_student } });
    out[2] = timeQuery(dataset, io, "?s teacherOf ?o", .{ .p = pred_teacher_of });
    out[3] = timeQuery(dataset, io, "u0/d0 ?p ?o", .{ .s = .{ .iri = ns_inst ++ "u0/d0" } });
    out[4] = timeQuery(dataset, io, "?s ?p ?o (default graph)", .{ .g = graph_default });
    out[5] = timeIteration(dataset, io);
}

fn timeQuery(dataset: *const RDFDataset, io: Io, label: []const u8, pattern: libderive.Pattern) QueryResult {
    const start = nowMonotonic(io);
    var iterator = dataset.match(pattern);
    var matches: u64 = 0;
    while (iterator.next()) |_| matches += 1;
    return .{ .label = label, .matches = matches, .elapsed_ns = elapsedNanos(start, nowMonotonic(io)) };
}

fn timeIteration(dataset: *const RDFDataset, io: Io) QueryResult {
    const start = nowMonotonic(io);
    var iterator = dataset.iterStatements();
    var matches: u64 = 0;
    while (iterator.next()) |_| matches += 1;
    return .{
        .label = "iterStatements (full scan)",
        .matches = matches,
        .elapsed_ns = elapsedNanos(start, nowMonotonic(io)),
    };
}

fn fmtMillis(nanoseconds: u64) f64 {
    return @as(f64, @floatFromInt(nanoseconds)) / 1_000_000.0;
}

/// Run `load` + `runQuerySuite` for each scale and emit plain key=value
/// log lines (one metric per line) so machine- and eyeball-parsing stay easy.
pub fn bench(init_process: std.process.Init, backing: libderive.IndexBacking, scales: []const u64) !void {
    std.log.info("benchmark=lubm backing={s}", .{@tagName(backing)});

    for (scales) |scale| {
        var dataset = try RDFDataset.init(init_process, .memory, backing);
        defer dataset.deinit();

        const load_stats = try load(&dataset, init_process.io, scale);
        var query_results: [6]QueryResult = undefined;
        runQuerySuite(&dataset, init_process.io, &query_results);

        var total_query_ns: u64 = 0;
        for (query_results) |result| total_query_ns += result.elapsed_ns;
        const mean_query_ns = total_query_ns / query_results.len;
        const qps = if (load_stats.elapsed_ns == 0) 0.0 else @as(f64, @floatFromInt(load_stats.inserted)) * 1_000_000_000.0 / @as(f64, @floatFromInt(load_stats.elapsed_ns));

        std.log.info("scale={d} quads={d} universities={d} load_ms={d:.3} load_qps={d:.0} mean_query_ms={d:.3}", .{
            scale,
            load_stats.inserted,
            load_stats.universities,
            fmtMillis(load_stats.elapsed_ns),
            qps,
            fmtMillis(mean_query_ns),
        });

        for (query_results) |result| {
            std.log.info("  query=\"{s}\" matches={d} elapsed_ms={d:.3}", .{
                result.label,
                result.matches,
                fmtMillis(result.elapsed_ns),
            });
        }
    }
}

/// Separately exercise WAL durability with a small LUBM fragment. Not timed
/// so journal IO does not pollute the in-memory benchmark numbers above.
pub fn walRoundtrip(init_process: std.process.Init, backing: libderive.IndexBacking, wal_path: []const u8) !void {
    std.Io.Dir.cwd().deleteFile(init_process.io, wal_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    var seeded: u64 = 0;
    {
        var dataset = try RDFDataset.init(init_process, .{ .journal = wal_path }, backing);
        defer dataset.deinit();
        _ = try load(&dataset, init_process.io, 2_000);
        try dataset.commitWal();
        seeded = dataset.statementCount();
    }
    var replayed = try RDFDataset.init(init_process, .{ .journal = wal_path }, backing);
    defer replayed.deinit();
    std.log.info("wal_path={s} seeded={d} replayed={d}", .{
        wal_path,
        seeded,
        replayed.statementCount(),
    });
}
