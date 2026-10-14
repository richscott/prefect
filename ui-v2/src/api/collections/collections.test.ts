import { QueryClient, useSuspenseQuery } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { buildApiUrl, createWrapper, server } from "@tests/utils";
import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";
import { buildListWorkPoolTypesQuery, queryKeyFactory } from "./collections";

describe("workers api", () => {
	const mockWorkersResponse = {
		prefect: {
			process: {
				type: "process",
				display_name: "Process",
				description: "Execute flow runs as subprocesses",
				logo_url: "https://example.com/process.png",
				is_beta: false,
				default_base_job_configuration: {
					job_configuration: {
						command: "{{ command }}",
						env: "{{ env }}",
					},
					variables: {
						properties: {
							command: { type: "string" },
							env: { type: "object" },
						},
					},
				},
			},
		},
		"prefect-aws": {
			ecs: {
				type: "ecs",
				display_name: "AWS ECS",
				description: "Execute flow runs on AWS ECS",
				logo_url: "https://example.com/ecs.png",
				is_beta: false,
				default_base_job_configuration: {
					job_configuration: {
						image: "{{ image }}",
					},
					variables: {
						properties: {
							image: { type: "string" },
						},
					},
				},
			},
		},
	};

	describe("buildListWorkPoolTypesQuery", () => {
		it("fetches work pool types metadata", async () => {
			server.use(
				http.get(
					buildApiUrl("/collections/views/aggregate-worker-metadata"),
					() => {
						return HttpResponse.json(mockWorkersResponse);
					},
				),
			);

			const queryClient = new QueryClient();
			const { result } = renderHook(
				() => useSuspenseQuery(buildListWorkPoolTypesQuery()),
				{ wrapper: createWrapper({ queryClient }) },
			);

			await waitFor(() => expect(result.current.isSuccess).toBe(true));
			expect(result.current.data).toEqual(mockWorkersResponse);
		});

		it("is shared by every consumer rather than refetched per caller", async () => {
			let requests = 0;
			server.use(
				http.get(
					buildApiUrl("/collections/views/aggregate-worker-metadata"),
					() => {
						requests += 1;
						return HttpResponse.json(mockWorkersResponse);
					},
				),
			);

			const queryClient = new QueryClient();
			const wrapper = createWrapper({ queryClient });
			const { result } = renderHook(
				() => {
					// Stands in for the many cards, badges and forms that each ask for
					// worker metadata while one work pool page is open.
					useSuspenseQuery(buildListWorkPoolTypesQuery());
					useSuspenseQuery(buildListWorkPoolTypesQuery());
					return useSuspenseQuery(buildListWorkPoolTypesQuery());
				},
				{ wrapper },
			);

			await waitFor(() => expect(result.current.isSuccess).toBe(true));

			renderHook(() => useSuspenseQuery(buildListWorkPoolTypesQuery()), {
				wrapper,
			});

			await waitFor(() => expect(requests).toBe(1));
			expect(
				queryClient.getQueryState(queryKeyFactory.workPoolTypes())
					?.dataUpdatedAt,
			).toBeGreaterThan(0);
		});

		it("stays fresh long enough to avoid refetching on focus", () => {
			expect(buildListWorkPoolTypesQuery().staleTime).toBe(10 * 60_000);
		});

		it("handles empty response", async () => {
			server.use(
				http.get(
					buildApiUrl("/collections/views/aggregate-worker-metadata"),
					() => {
						return HttpResponse.json({});
					},
				),
			);

			const queryClient = new QueryClient();
			const { result } = renderHook(
				() => useSuspenseQuery(buildListWorkPoolTypesQuery()),
				{ wrapper: createWrapper({ queryClient }) },
			);

			await waitFor(() => expect(result.current.isSuccess).toBe(true));
			expect(result.current.data).toEqual({});
		});
	});
});
