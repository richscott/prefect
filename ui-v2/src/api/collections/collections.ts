import { queryOptions } from "@tanstack/react-query";
import { getQueryService } from "@/api/service";

/**
 * Query key factory for workers-related queries
 */
export const queryKeyFactory = {
	all: () => ["collections"] as const,
	workPoolTypes: () => [...queryKeyFactory.all(), "work-pool-types"] as const,
};

/**
 * Builds a query configuration for fetching the work pool types collection
 *
 * @returns Query configuration object for use with TanStack Query
 *
 * @example
 * ```ts
 * const query = useSuspenseQuery(buildListWorkPoolTypesQuery());
 * ```
 */
export const buildListWorkPoolTypesQuery = () =>
	queryOptions({
		queryKey: queryKeyFactory.workPoolTypes(),
		// Worker metadata only changes when an integration is installed or upgraded
		// on the server, which requires a restart. The server caches this view for
		// ten minutes, so matching that here avoids refetching on every window focus
		// and keeps many work pool cards sharing a single request.
		staleTime: 10 * 60_000,
		queryFn: async () => {
			const res = await (await getQueryService()).GET(
				"/collections/views/{view}",
				{
					params: { path: { view: "aggregate-worker-metadata" } },
				},
			);
			if (!res.data) {
				throw new Error("'data' expected");
			}
			return res.data;
		},
	});
