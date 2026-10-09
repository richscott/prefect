import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { buildListWorkPoolTypesQuery } from "@/api/collections/collections";
import type { WorkPool } from "@/api/work-pools";
import { Badge } from "@/components/ui/badge";
import { Icon } from "@/components/ui/icons";
import {
	parseWorkerMetadata,
	workerDisplayName,
} from "@/components/work-pools/worker-metadata";

type WorkPoolTypeBadgeProps = {
	type: WorkPool["type"];
};

export const WorkPoolTypeBadge = ({ type }: WorkPoolTypeBadgeProps) => {
	// Non-suspending: a work pool list must stay readable while worker metadata
	// is loading, and remain usable if it never arrives. Every badge on the page
	// shares this one query.
	const { data } = useQuery(buildListWorkPoolTypesQuery());
	const label = useMemo(
		() => workerDisplayName(type, parseWorkerMetadata(data)),
		[type, data],
	);

	return (
		<Badge>
			<Icon id="Cpu" />
			{label}
		</Badge>
	);
};
