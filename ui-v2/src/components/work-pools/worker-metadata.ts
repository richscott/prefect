import { z } from "zod";
import type { WorkerBaseJobTemplate } from "@/components/work-pools/types";
import { titleCase } from "@/utils";

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
	typeof value === "object" && value !== null && !Array.isArray(value);

/** Rejects arrays and `null`, which `typeof` alone would report as objects. */
const plainObject = z.custom<Record<string, unknown>>(isPlainObject);

/**
 * The template is a JSON schema the schema form consumes whole, so only the
 * shape `WorkerBaseJobTemplate` declares is checked. Its contents (`$defs`,
 * `$ref`, defaults, and keywords this UI does not recognize) are carried
 * through untouched.
 */
const workerBaseJobTemplate = z.custom<WorkerBaseJobTemplate>(
	(value) =>
		isPlainObject(value) &&
		(value.job_configuration === undefined ||
			isPlainObject(value.job_configuration)) &&
		(value.variables === undefined || isPlainObject(value.variables)),
);

/** Present and non-blank, preserved exactly as the server sent it. */
const nonBlankString = z.string().refine((value) => value.trim().length > 0);

/**
 * Optional presentation. `.catch` turns anything unusable (`null`, a number, a
 * blank string) into an absent value instead of failing the whole record, so
 * one bad field cannot hide an otherwise usable worker type.
 */
const optionalText = nonBlankString.optional().catch(undefined);

const workerMetadataSchema = z.object({
	type: nonBlankString,
	display_name: optionalText,
	description: optionalText,
	documentation_url: optionalText,
	logo_url: optionalText,
	// Beta is a badge, so only a real `true` earns it; strings are not coerced.
	is_beta: z.boolean().default(false).catch(false),
	default_base_job_configuration: workerBaseJobTemplate
		.optional()
		.catch(undefined),
});

export type WorkerMetadata = z.infer<typeof workerMetadataSchema>;

/**
 * Builds a lookup of worker metadata keyed by each record's own `type`.
 *
 * The `aggregate-worker-metadata` view is keyed by collection name and then by
 * worker type, mixing the published registry with whatever integrations are
 * installed next to the server. The key a record sits under is the collection's
 * idea of its name and is not always the worker type, so the `type` the record
 * carries is used instead. Records that cannot be understood are skipped rather
 * than discarding their siblings.
 */
export function parseWorkerMetadata(
	input: unknown,
): Map<string, WorkerMetadata> {
	const workers = new Map<string, WorkerMetadata>();

	const response = plainObject.safeParse(input);
	if (!response.success) {
		return workers;
	}

	for (const collection of Object.values(response.data)) {
		const parsedCollection = plainObject.safeParse(collection);
		if (!parsedCollection.success) {
			continue;
		}

		for (const record of Object.values(parsedCollection.data)) {
			const parsedRecord = workerMetadataSchema.safeParse(record);
			if (!parsedRecord.success) {
				continue;
			}

			// First valid record wins, so repeated reads of one response are stable.
			// Reconciling genuine duplicates is the server's job, not the UI's.
			if (!workers.has(parsedRecord.data.type)) {
				workers.set(parsedRecord.data.type, parsedRecord.data);
			}
		}
	}

	return workers;
}

/**
 * The label to show for a worker type, whether or not its integration is
 * installed, published, or known to this UI at all.
 */
export function workerDisplayName(
	type: string,
	metadata?: Map<string, WorkerMetadata>,
): string {
	return metadata?.get(type)?.display_name ?? titleCase(type);
}
