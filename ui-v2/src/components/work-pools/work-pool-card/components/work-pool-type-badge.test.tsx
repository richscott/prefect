import { QueryClient } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { buildApiUrl, createWrapper, server } from "@tests/utils";
import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";
import { WorkPoolTypeBadge } from "./work-pool-type-badge";

const metadataResponse = {
	"prefect-fixture": {
		// Keyed by the collection's own name for the record, not the worker type.
		"fixture-alias": {
			type: "fixture-worker",
			display_name: "Fixture Infrastructure",
		},
	},
};

const newQueryClient = () =>
	new QueryClient({ defaultOptions: { queries: { retry: false } } });

const serveMetadata = (handler: () => Response) =>
	server.use(
		http.get(buildApiUrl("/collections/views/aggregate-worker-metadata"), () =>
			handler(),
		),
	);

describe("WorkPoolTypeBadge", () => {
	it("shows the display name registered by the integration", async () => {
		serveMetadata(() => HttpResponse.json(metadataResponse));

		render(<WorkPoolTypeBadge type="fixture-worker" />, {
			wrapper: createWrapper({ queryClient: newQueryClient() }),
		});

		expect(
			await screen.findByText("Fixture Infrastructure"),
		).toBeInTheDocument();
	});

	it("falls back to the raw type while metadata is loading", async () => {
		serveMetadata(() => HttpResponse.json(metadataResponse));

		render(<WorkPoolTypeBadge type="fixture-worker" />, {
			wrapper: createWrapper({ queryClient: newQueryClient() }),
		});

		// Rendered immediately rather than suspending the surrounding list.
		expect(screen.getByText("Fixture Worker")).toBeInTheDocument();
		expect(
			await screen.findByText("Fixture Infrastructure"),
		).toBeInTheDocument();
	});

	it("stays readable when metadata cannot be loaded", async () => {
		serveMetadata(() => new HttpResponse(null, { status: 503 }));

		render(<WorkPoolTypeBadge type="mystery-worker" />, {
			wrapper: createWrapper({ queryClient: newQueryClient() }),
		});

		await waitFor(() =>
			expect(screen.getByText("Mystery Worker")).toBeInTheDocument(),
		);
	});

	it("shows an uninstalled type by its raw identifier", async () => {
		serveMetadata(() => HttpResponse.json(metadataResponse));

		render(<WorkPoolTypeBadge type="uninstalled-worker" />, {
			wrapper: createWrapper({ queryClient: newQueryClient() }),
		});

		expect(await screen.findByText("Uninstalled Worker")).toBeInTheDocument();
	});

	it("shares one metadata request across many badges", async () => {
		let requests = 0;
		serveMetadata(() => {
			requests += 1;
			return HttpResponse.json(metadataResponse);
		});

		const wrapper = createWrapper({ queryClient: newQueryClient() });
		render(
			<>
				{Array.from({ length: 20 }, (_, index) => (
					<WorkPoolTypeBadge key={index} type="fixture-worker" />
				))}
			</>,
			{ wrapper },
		);

		await waitFor(() =>
			expect(screen.getAllByText("Fixture Infrastructure")).toHaveLength(20),
		);
		expect(requests).toBe(1);
	});
});
