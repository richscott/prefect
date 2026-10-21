import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { WorkPoolCreateWizard } from "./work-pool-create-wizard";

// Mock data for work pool types
const mockWorkPoolTypes = {
	prefect: {
		process: {
			type: "process",
			display_name: "Process",
			description: "Execute flow runs within subprocesses.",
			logo_url: "https://example.com/logo.svg",
			documentation_url: "https://docs.prefect.io/",
			is_beta: false,
		},
	},
	docker: {
		"docker-container": {
			type: "docker-container",
			display_name: "Docker Container",
			description: "Execute flow runs in Docker containers.",
			logo_url: "https://example.com/docker.svg",
			documentation_url: "https://docs.prefect.io/docker",
			is_beta: false,
		},
	},
};

// Mock the router
vi.mock("@tanstack/react-router", () => ({
	useRouter: () => ({
		navigate: vi.fn(),
	}),
}));

// Mock the API hooks
const { createWorkPoolMock } = vi.hoisted(() => ({
	createWorkPoolMock: vi.fn(),
}));

vi.mock("@/api/work-pools", () => ({
	useCreateWorkPool: () => ({
		createWorkPool: createWorkPoolMock,
		isPending: false,
	}),
}));

// Mock the collections API
vi.mock("@/api/collections/collections", () => ({
	buildListWorkPoolTypesQuery: () => ({
		queryKey: ["work-pool-types"],
		queryFn: () => mockWorkPoolTypes,
	}),
}));

// Mock the toast
vi.mock("sonner", () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn(),
	},
}));

describe("WorkPoolCreateWizard", () => {
	let queryClient: QueryClient;

	beforeEach(() => {
		queryClient = new QueryClient({
			defaultOptions: {
				queries: { retry: false, staleTime: Number.POSITIVE_INFINITY },
				mutations: { retry: false },
			},
		});

		// Pre-populate the query client with mock data to avoid suspense
		queryClient.setQueryData(["work-pool-types"], mockWorkPoolTypes);

		vi.clearAllMocks();
	});

	const renderWorkPoolCreateWizard = () => {
		return render(
			<QueryClientProvider client={queryClient}>
				<WorkPoolCreateWizard />
			</QueryClientProvider>,
		);
	};

	it("renders the wizard with initial step", () => {
		renderWorkPoolCreateWizard();

		expect(screen.getByText("Create Work Pool")).toBeInTheDocument();
		expect(screen.getByText("Infrastructure Type")).toBeInTheDocument();
		expect(screen.getByText("Details")).toBeInTheDocument();
		expect(screen.getByText("Configuration")).toBeInTheDocument();
	});

	it("shows navigation buttons correctly", () => {
		renderWorkPoolCreateWizard();

		// On first step, only Next and Cancel should be visible
		expect(screen.queryByText("Back")).not.toBeInTheDocument();
		expect(screen.getByText("Next")).toBeInTheDocument();
		expect(screen.getByText("Cancel")).toBeInTheDocument();
	});

	it("renders infrastructure type options", () => {
		renderWorkPoolCreateWizard();

		expect(screen.getByText("Process")).toBeInTheDocument();
		expect(screen.getByText("Docker Container")).toBeInTheDocument();
		expect(
			screen.getByText("Execute flow runs in Docker containers."),
		).toBeInTheDocument();
	});

	it("shows form validation message when trying to proceed without selection", async () => {
		renderWorkPoolCreateWizard();

		const nextButton = screen.getByText("Next");
		fireEvent.click(nextButton);

		// Should show validation error
		await waitFor(() => {
			// The form should still be on the first step and show validation
			expect(screen.getByText("Infrastructure Type")).toBeInTheDocument();
		});
	});

	it("renders Cancel button", () => {
		renderWorkPoolCreateWizard();

		const cancelButton = screen.getByText("Cancel");
		expect(cancelButton).toBeInTheDocument();
	});
});

/**
 * A collection that keys its record by something other than the worker type it
 * carries, which is what an installed-but-unpublished integration looks like.
 */
const aliasedWorkPoolTypes = {
	"prefect-fixture": {
		"fixture-alias": {
			type: "fixture-worker",
			display_name: "Fixture Worker",
			description: "Runs flows on fixture infrastructure.",
			is_beta: false,
			default_base_job_configuration: {
				job_configuration: { command: "{{ command }}" },
				variables: {
					type: "object",
					properties: {
						command: {
							type: "string",
							title: "Command",
							default: "fixture-default",
						},
					},
				},
			},
		},
	},
	"prefect-other": {
		other: {
			type: "other-worker",
			display_name: "Other Worker",
			is_beta: false,
			default_base_job_configuration: {
				job_configuration: { image: "{{ image }}" },
				variables: {
					type: "object",
					properties: {
						image: {
							type: "string",
							title: "Image",
							default: "other-default",
						},
					},
				},
			},
		},
	},
	"prefect-bare": {
		bare: { type: "bare-worker" },
	},
};

describe("WorkPoolCreateWizard configuration from worker metadata", () => {
	let queryClient: QueryClient;

	beforeEach(() => {
		queryClient = new QueryClient({
			defaultOptions: {
				queries: { retry: false, staleTime: Number.POSITIVE_INFINITY },
				mutations: { retry: false },
			},
		});
		queryClient.setQueryData(
			["work-pool-types"],
			structuredClone(aliasedWorkPoolTypes),
		);
		vi.clearAllMocks();
	});

	const renderWizard = () =>
		render(
			<QueryClientProvider client={queryClient}>
				<WorkPoolCreateWizard />
			</QueryClientProvider>,
		);

	const selectType = (name: RegExp) =>
		fireEvent.click(screen.getByRole("radio", { name }));

	const clickNext = () => fireEvent.click(screen.getByText("Next"));

	const goBack = async () => fireEvent.click(await screen.findByText("Back"));

	// Step advances happen after an async validation pass, so every helper waits
	// for the step it is moving to before returning.
	const reachConfigurationStep = () =>
		screen.findByRole("tab", { name: "Defaults" });

	const fillNameAndContinue = async (name: string) => {
		clickNext();
		const nameInput = await screen.findByLabelText("Name");
		fireEvent.change(nameInput, { target: { value: name } });
		clickNext();
		await reachConfigurationStep();
	};

	const commandInput = () => screen.findByLabelText(/^Command/);
	const imageInput = () => screen.findByLabelText(/^Image/);

	it("initializes the template from the record type, not the key it sits under", async () => {
		renderWizard();

		selectType(/fixture worker/i);
		await fillNameAndContinue("aliased-pool");

		// The old lookup indexed the collection by the selected type, so an aliased
		// key silently produced an empty configuration.
		expect(await commandInput()).toHaveValue("fixture-default");
	});

	it("submits the selected type and the user's edited defaults", async () => {
		renderWizard();

		selectType(/fixture worker/i);
		await fillNameAndContinue("aliased-pool");

		fireEvent.change(await commandInput(), { target: { value: "edited" } });
		fireEvent.click(screen.getByRole("button", { name: "Create Work Pool" }));

		await waitFor(() => expect(createWorkPoolMock).toHaveBeenCalled());

		const [payload] = createWorkPoolMock.mock.calls[0] as [
			{
				name: string;
				type: string;
				base_job_template: {
					variables: { properties: { command: { default: string } } };
				};
			},
		];
		expect(payload.name).toBe("aliased-pool");
		expect(payload.type).toBe("fixture-worker");
		expect(payload.base_job_template.variables.properties.command.default).toBe(
			"edited",
		);
	});

	it("does not mutate the cached metadata response", async () => {
		const pristine = structuredClone(aliasedWorkPoolTypes);
		renderWizard();

		selectType(/fixture worker/i);
		await fillNameAndContinue("aliased-pool");
		fireEvent.change(await commandInput(), { target: { value: "edited" } });

		expect(queryClient.getQueryData(["work-pool-types"])).toEqual(pristine);
	});

	it("loads fresh defaults when the type changes and changes back", async () => {
		renderWizard();

		selectType(/fixture worker/i);
		await fillNameAndContinue("round-trip-pool");
		fireEvent.change(await commandInput(), { target: { value: "edited" } });

		// A -> B
		await goBack();
		await goBack();
		selectType(/other worker/i);
		clickNext();
		clickNext();
		await reachConfigurationStep();
		expect(await imageInput()).toHaveValue("other-default");

		// B -> A discards the edit made before the switch
		await goBack();
		await goBack();
		selectType(/fixture worker/i);
		clickNext();
		clickNext();
		await reachConfigurationStep();
		expect(await commandInput()).toHaveValue("fixture-default");
	});

	it("keeps the details entered on the way through a type change", async () => {
		renderWizard();

		selectType(/fixture worker/i);
		await fillNameAndContinue("kept-name");

		await goBack();
		await goBack();
		selectType(/other worker/i);
		clickNext();

		expect(await screen.findByLabelText("Name")).toHaveValue("kept-name");
	});

	it("preserves edits when the metadata response is replaced", async () => {
		renderWizard();

		selectType(/fixture worker/i);
		await fillNameAndContinue("refetch-pool");
		fireEvent.change(await commandInput(), { target: { value: "edited" } });

		// Step back to where the picker subscribes to the query, then land a fresh
		// response in the cache exactly as a refetch would.
		await goBack();
		await goBack();
		await screen.findByRole("radio", { name: /fixture worker/i });
		queryClient.setQueryData(
			["work-pool-types"],
			structuredClone(aliasedWorkPoolTypes),
		);

		clickNext();
		clickNext();
		await reachConfigurationStep();

		expect(await commandInput()).toHaveValue("edited");
	});

	it("can configure a worker whose metadata carries only a type", async () => {
		renderWizard();

		selectType(/bare worker/i);
		await fillNameAndContinue("bare-pool");
		fireEvent.click(screen.getByRole("button", { name: "Create Work Pool" }));

		await waitFor(() => expect(createWorkPoolMock).toHaveBeenCalled());

		const [payload] = createWorkPoolMock.mock.calls[0] as [
			{ type: string; base_job_template: Record<string, unknown> },
		];
		expect(payload.type).toBe("bare-worker");
		expect(payload.base_job_template).toEqual({});
	});
});
