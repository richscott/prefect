import { describe, expect, it } from "vitest";
import { parseWorkerMetadata, workerDisplayName } from "./worker-metadata";

const fullRecord = {
	type: "fixture-worker",
	display_name: "Fixture Worker",
	description: "Runs flows on fixture infrastructure.",
	documentation_url: "https://example.com/docs",
	logo_url: "data:image/svg+xml;base64,PHN2Zy8+",
	is_beta: true,
	default_base_job_configuration: {
		job_configuration: { command: "{{ command }}" },
		variables: {
			type: "object",
			properties: { command: { type: "string", default: "run" } },
			$defs: { Extra: { type: "object", additionalProperties: false } },
		},
	},
};

const response = { "prefect-fixture": { "fixture-worker": fullRecord } };

describe("parseWorkerMetadata", () => {
	it("indexes records by their own type", () => {
		const workers = parseWorkerMetadata(response);

		expect([...workers.keys()]).toEqual(["fixture-worker"]);
		expect(workers.get("fixture-worker")?.display_name).toBe("Fixture Worker");
	});

	it("indexes by the record type rather than the key it sits under", () => {
		const workers = parseWorkerMetadata({
			"prefect-fixture": { "an-alias-key": { ...fullRecord } },
		});

		expect(workers.has("fixture-worker")).toBe(true);
		expect(workers.has("an-alias-key")).toBe(false);
	});

	it("accepts a record carrying nothing but a type", () => {
		const workers = parseWorkerMetadata({
			"prefect-fixture": { minimal: { type: "minimal-worker" } },
		});

		expect(workers.get("minimal-worker")).toEqual({
			type: "minimal-worker",
			is_beta: false,
		});
	});

	it("preserves the template exactly, including nested schema keywords", () => {
		const template =
			parseWorkerMetadata(response).get(
				"fixture-worker",
			)?.default_base_job_configuration;

		expect(template).toEqual(fullRecord.default_base_job_configuration);
		expect(template?.variables).toHaveProperty("$defs.Extra");
	});

	it("does not mutate the response it was given", () => {
		const input = structuredClone(response);

		parseWorkerMetadata(input);

		expect(input).toEqual(response);
	});

	it.each([null, undefined, 42, "text", [fullRecord]])(
		"returns an empty map for a malformed response (%s)",
		(input) => {
			expect(parseWorkerMetadata(input).size).toBe(0);
		},
	);

	it("skips malformed collections but keeps usable ones", () => {
		const workers = parseWorkerMetadata({
			broken: [fullRecord],
			alsoBroken: null,
			"prefect-fixture": { "fixture-worker": fullRecord },
		});

		expect([...workers.keys()]).toEqual(["fixture-worker"]);
	});

	it.each([
		["a missing type", { display_name: "No Type" }],
		["a blank type", { type: "   " }],
		["a non-string type", { type: 7 }],
		["a non-object record", "not-a-record"],
	])("skips a record with %s", (_label, record) => {
		const workers = parseWorkerMetadata({
			"prefect-fixture": { bad: record, good: fullRecord },
		});

		expect([...workers.keys()]).toEqual(["fixture-worker"]);
	});

	it.each([null, 42, "", "   ", {}])(
		"treats unusable optional presentation (%s) as absent",
		(value) => {
			const workers = parseWorkerMetadata({
				"prefect-fixture": {
					"fixture-worker": {
						type: "fixture-worker",
						display_name: value,
						description: value,
						documentation_url: value,
						logo_url: value,
					},
				},
			});

			const worker = workers.get("fixture-worker");
			expect(worker).toBeDefined();
			expect(worker?.display_name).toBeUndefined();
			expect(worker?.description).toBeUndefined();
			expect(worker?.documentation_url).toBeUndefined();
			expect(worker?.logo_url).toBeUndefined();
		},
	);

	it.each([
		[true, true],
		[false, false],
		["true", false],
		[1, false],
		[undefined, false],
	])("reads is_beta %s as %s", (input, expected) => {
		const workers = parseWorkerMetadata({
			"prefect-fixture": {
				"fixture-worker": { type: "fixture-worker", is_beta: input },
			},
		});

		expect(workers.get("fixture-worker")?.is_beta).toBe(expected);
	});

	it.each([null, 42, "template", [], { variables: 7 }])(
		"treats an unusable template (%s) as absent without dropping the worker",
		(template) => {
			const workers = parseWorkerMetadata({
				"prefect-fixture": {
					"fixture-worker": {
						type: "fixture-worker",
						default_base_job_configuration: template,
					},
				},
			});

			expect(workers.get("fixture-worker")).toBeDefined();
			expect(
				workers.get("fixture-worker")?.default_base_job_configuration,
			).toBeUndefined();
		},
	);

	it("keeps the first valid record when a type appears twice", () => {
		const workers = parseWorkerMetadata({
			"collection-a": {
				"fixture-worker": { type: "fixture-worker", display_name: "First" },
			},
			"collection-b": {
				"fixture-worker": { type: "fixture-worker", display_name: "Second" },
			},
		});

		expect(workers.get("fixture-worker")?.display_name).toBe("First");
	});
});

describe("workerDisplayName", () => {
	it("uses the registered display name", () => {
		expect(
			workerDisplayName("fixture-worker", parseWorkerMetadata(response)),
		).toBe("Fixture Worker");
	});

	it("falls back to a title-cased type when metadata has no name", () => {
		const workers = parseWorkerMetadata({
			"prefect-fixture": { minimal: { type: "fixture-worker" } },
		});

		expect(workerDisplayName("fixture-worker", workers)).toBe("Fixture Worker");
	});

	it("falls back when the type is unknown or metadata is unavailable", () => {
		expect(workerDisplayName("mystery-worker", new Map())).toBe(
			"Mystery Worker",
		);
		expect(workerDisplayName("mystery-worker")).toBe("Mystery Worker");
	});
});
