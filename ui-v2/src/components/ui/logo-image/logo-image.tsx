import type React from "react";
import { useState } from "react";
import { cn } from "@/utils";

type LogoImageProps = {
	url: string | null;
	alt: string;
	size?: "sm" | "md" | "lg";
	className?: string;
};

const sizeClasses = {
	sm: "h-4 w-4",
	md: "h-8 w-8",
	lg: "h-12 w-12",
};

type LogoFallbackProps = Omit<LogoImageProps, "url">;

const LogoFallback = ({ alt, size = "md", className }: LogoFallbackProps) => (
	<div
		className={cn(
			"rounded border bg-muted flex items-center justify-center text-muted-foreground text-xs",
			sizeClasses[size],
			className,
		)}
	>
		{alt.charAt(0).toUpperCase()}
	</div>
);

/**
 * Keyed by `url` from the outer component, so a new image gets a fresh attempt
 * rather than inheriting the previous one's failure.
 */
const LogoImageWithFallback = ({
	url,
	alt,
	size = "md",
	className,
}: LogoImageProps & { url: string }) => {
	const [hasError, setHasError] = useState(false);

	if (hasError) {
		return <LogoFallback alt={alt} size={size} className={className} />;
	}

	return (
		<img
			src={url}
			alt={alt}
			className={cn(
				"rounded border bg-background/50 backdrop-blur-sm object-contain",
				sizeClasses[size],
				className,
			)}
			onError={() => setHasError(true)}
		/>
	);
};

export const LogoImage: React.FC<LogoImageProps> = ({
	url,
	alt,
	size = "md",
	className,
}) => {
	if (!url) {
		return <LogoFallback alt={alt} size={size} className={className} />;
	}

	return (
		<LogoImageWithFallback
			key={url}
			url={url}
			alt={alt}
			size={size}
			className={className}
		/>
	);
};
