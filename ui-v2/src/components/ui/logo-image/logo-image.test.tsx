import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { LogoImage } from "./logo-image";

describe("LogoImage", () => {
	it("renders image when url is provided", () => {
		render(
			<LogoImage
				url="https://example.com/logo.png"
				alt="Test Logo"
				size="md"
			/>,
		);

		const image = screen.getByRole("img");
		expect(image).toBeInTheDocument();
		expect(image).toHaveAttribute("src", "https://example.com/logo.png");
		expect(image).toHaveAttribute("alt", "Test Logo");
	});

	it("renders fallback when url is null", () => {
		render(<LogoImage url={null} alt="Test Logo" size="md" />);

		expect(screen.getByText("T")).toBeInTheDocument();
		expect(screen.queryByRole("img")).not.toBeInTheDocument();
	});

	it("applies correct size classes", () => {
		const { rerender } = render(
			<LogoImage url="https://example.com/logo.png" alt="Test" size="sm" />,
		);

		let image = screen.getByRole("img");
		expect(image).toHaveClass("h-4", "w-4");

		rerender(
			<LogoImage url="https://example.com/logo.png" alt="Test" size="md" />,
		);
		image = screen.getByRole("img");
		expect(image).toHaveClass("h-8", "w-8");

		rerender(
			<LogoImage url="https://example.com/logo.png" alt="Test" size="lg" />,
		);
		image = screen.getByRole("img");
		expect(image).toHaveClass("h-12", "w-12");
	});

	it("applies custom className", () => {
		render(
			<LogoImage
				url="https://example.com/logo.png"
				alt="Test"
				size="md"
				className="custom-class"
			/>,
		);

		const image = screen.getByRole("img");
		expect(image).toHaveClass("custom-class");
	});

	it("falls back when the image fails to load", () => {
		render(<LogoImage url="https://example.com/missing.png" alt="Test Logo" />);

		fireEvent.error(screen.getByRole("img"));

		expect(screen.getByText("T")).toBeInTheDocument();
		expect(screen.queryByRole("img")).not.toBeInTheDocument();
	});

	it("retries when the url changes after a failure", () => {
		const { rerender } = render(
			<LogoImage url="https://example.com/missing.png" alt="Test Logo" />,
		);

		fireEvent.error(screen.getByRole("img"));
		expect(screen.queryByRole("img")).not.toBeInTheDocument();

		rerender(
			<LogoImage url="https://example.com/present.png" alt="Test Logo" />,
		);

		expect(screen.getByRole("img")).toHaveAttribute(
			"src",
			"https://example.com/present.png",
		);
	});

	it("renders a data url like any other image source", () => {
		const dataUrl = "data:image/svg+xml;base64,PHN2Zy8+";
		render(<LogoImage url={dataUrl} alt="Packaged Logo" />);

		expect(screen.getByRole("img")).toHaveAttribute("src", dataUrl);
	});

	it("keeps alt and size updates working after an error", () => {
		const { rerender } = render(
			<LogoImage url="https://example.com/a.png" alt="Alpha" size="sm" />,
		);
		fireEvent.error(screen.getByRole("img"));
		expect(screen.getByText("A")).toBeInTheDocument();

		rerender(
			<LogoImage url="https://example.com/a.png" alt="Beta" size="lg" />,
		);

		const fallback = screen.getByText("B");
		expect(fallback).toHaveClass("h-12", "w-12");
	});
});
