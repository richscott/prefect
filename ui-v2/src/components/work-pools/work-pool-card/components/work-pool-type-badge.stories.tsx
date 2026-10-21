import type { Meta, StoryObj } from "@storybook/react";
import { buildApiUrl } from "@tests/utils/handlers";
import { HttpResponse, http } from "msw";
import { reactQueryDecorator } from "@/storybook/utils";
import { WorkPoolTypeBadge } from "./work-pool-type-badge";

const workerMetadata = {
	prefect: { process: { type: "process", display_name: "Process" } },
	"prefect-kubernetes": {
		kubernetes: { type: "kubernetes", display_name: "Kubernetes" },
	},
	"prefect-docker": {
		docker: { type: "docker", display_name: "Docker" },
	},
	"prefect-aws": { ecs: { type: "ecs", display_name: "AWS ECS" } },
};

const meta: Meta<typeof WorkPoolTypeBadge> = {
	title: "Components/WorkPools/WorkPoolTypeBadge",
	component: WorkPoolTypeBadge,
	decorators: [reactQueryDecorator],
	parameters: {
		msw: {
			handlers: [
				http.get(
					buildApiUrl("/collections/views/aggregate-worker-metadata"),
					() => HttpResponse.json(workerMetadata),
				),
			],
		},
	},
	argTypes: {
		type: {
			control: "select",
			options: [
				"kubernetes",
				"process",
				"ecs",
				"azure-container-instance",
				"docker",
				"cloud-run",
				"cloud-run-v2",
				"vertex-ai",
				"kubernetes",
			],
			description: "The type of work pool to display a badge for",
		},
	},
};

export default meta;
type Story = StoryObj<typeof WorkPoolTypeBadge>;

export const Kubernetes: Story = {
	args: {
		type: "kubernetes",
	},
};

export const Process: Story = {
	args: {
		type: "process",
	},
};

export const Docker: Story = {
	args: {
		type: "docker",
	},
};

export const VertexAI: Story = {
	args: {
		type: "vertex-ai",
	},
};

export const ECS: Story = {
	args: {
		type: "ecs",
	},
};

export const AzureContainerInstance: Story = {
	args: {
		type: "azure-container-instance",
	},
};

export const CloudRun: Story = {
	args: {
		type: "cloud-run",
	},
};

export const CloudRunV2: Story = {
	args: {
		type: "cloud-run-v2",
	},
};

/** A type this UI knows nothing about still reads sensibly. */
export const UnknownType: Story = {
	args: {
		type: "some-unreleased-worker",
	},
};
