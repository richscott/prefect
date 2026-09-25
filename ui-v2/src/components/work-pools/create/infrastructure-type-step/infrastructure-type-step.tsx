import { useSuspenseQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { useFormContext } from "react-hook-form";
import { buildListWorkPoolTypesQuery } from "@/api/collections/collections";
import { Badge } from "@/components/ui/badge";
import {
	FormControl,
	FormField,
	FormItem,
	FormLabel,
	FormMessage,
} from "@/components/ui/form";
import { LogoImage } from "@/components/ui/logo-image";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
	parseWorkerMetadata,
	workerDisplayName,
} from "@/components/work-pools/worker-metadata";
import type { WorkPoolCreateFormValues } from "../work-pool-create-wizard";

type WorkPoolTypeSelectOption = {
	label: string;
	value: string;
	logoUrl: string | null;
	description: string | null;
	isBeta: boolean;
};

export function InfrastructureTypeStep() {
	const form = useFormContext<WorkPoolCreateFormValues>();
	const { data: workersResponse } = useSuspenseQuery(
		buildListWorkPoolTypesQuery(),
	);

	const workers = useMemo(
		() => parseWorkerMetadata(workersResponse),
		[workersResponse],
	);

	const options = useMemo<WorkPoolTypeSelectOption[]>(() => {
		const options = [...workers.values()].map((worker) => ({
			label: workerDisplayName(worker.type, workers),
			value: worker.type,
			// Only the type is required. A worker from a collection the registry does
			// not publish may carry no logo or description, and dropping it here
			// would hide a usable work pool type.
			logoUrl: worker.logo_url ?? null,
			description: worker.description ?? null,
			isBeta: worker.is_beta,
		}));

		// Sort options: non-beta first, then alphabetically by label
		return options.sort((firstOption, secondOption) => {
			if (firstOption.isBeta && !secondOption.isBeta) {
				return 1;
			}
			if (!firstOption.isBeta && secondOption.isBeta) {
				return -1;
			}
			return firstOption.label.localeCompare(secondOption.label);
		});
	}, [workers]);

	return (
		<FormField
			control={form.control}
			name="type"
			render={({ field, fieldState }) => (
				<FormItem>
					<FormLabel className="text-base font-medium">
						Select the infrastructure you want to use to execute your flow runs
					</FormLabel>
					<FormControl>
						<RadioGroup
							// An empty string keeps the group controlled before a selection.
							value={field.value ?? ""}
							onValueChange={(value: string) => {
								const worker = workers.get(value);
								// Selecting the same type again must not discard edits the
								// user has already made to its configuration.
								if (!worker || value === field.value) {
									return;
								}

								field.onChange(value);
								// The chosen worker's own defaults become the template, so
								// the previous worker's values never survive a switch.
								// Cloning keeps the cached query response immutable.
								form.setValue(
									"baseJobTemplate",
									structuredClone(worker.default_base_job_configuration ?? {}),
									{ shouldDirty: false, shouldTouch: false },
								);
								form.clearErrors("baseJobTemplate");
							}}
							className="space-y-3"
						>
							{options.map(({ label, value, logoUrl, description, isBeta }) => (
								<div
									key={value}
									className="flex items-center space-x-3 p-4 rounded-lg border hover:bg-accent/50"
								>
									<RadioGroupItem value={value} id={value} />
									<label htmlFor={value} className="flex-1 cursor-pointer">
										<div className="flex items-center gap-4">
											<LogoImage url={logoUrl} alt={label} size="md" />
											<div className="flex flex-col gap-2 flex-1">
												<p className="text-base font-medium flex items-center">
													{label}
													{isBeta && (
														<Badge variant="secondary" className="ml-2 text-xs">
															Beta
														</Badge>
													)}
												</p>
												{description && (
													<p className="text-sm text-muted-foreground">
														{description}
													</p>
												)}
											</div>
										</div>
									</label>
								</div>
							))}
						</RadioGroup>
					</FormControl>
					{fieldState.error && (
						<FormMessage>{fieldState.error.message}</FormMessage>
					)}
				</FormItem>
			)}
		/>
	);
}
