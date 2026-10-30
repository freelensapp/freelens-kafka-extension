// @vitest-environment jsdom

import { Common, type Renderer } from "@freelensapp/extensions";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ErrorBoundary,
  withDetailsErrorBoundary,
  withMenuItemErrorBoundary,
  withPageErrorBoundary,
} from "./error-boundary";

type DetailsProps = Renderer.Component.KubeObjectDetailsProps<Renderer.K8sApi.KubeObject>;
type MenuItemProps = Renderer.Component.KubeObjectMenuProps<Renderer.K8sApi.KubeObject>;

// A minimal stub extension - only `name` is read by the boundary.
const extension = { name: "kafka-extension" } as Renderer.LensExtension;

// A stand-in for a Kubernetes object: the stubs below read only `broken`.
const objectOf = (broken: boolean) => ({ broken }) as unknown as Renderer.K8sApi.KubeObject;
const isBroken = (object: Renderer.K8sApi.KubeObject) => (object as unknown as { broken: boolean }).broken;

beforeEach(() => {
  // The stub of the host's logger drops the message; the spy records it.
  vi.spyOn(Common.logger, "error");
  // React reports an error that a boundary caught on the console as well.
  vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.restoreAllMocks();
});

const Throws = ({ message = "boom" }: { message?: string }): React.JSX.Element => {
  throw new Error(message);
};

// A child that throws only for some objects, as a details panel does for an object without a field it reads.
const ThrowsFor = ({ object }: { object: Renderer.K8sApi.KubeObject }) => {
  if (isBroken(object)) throw new Error("broken object");
  return <p>fine object</p>;
};

// Small stand-ins for a page, a details panel and a menu item of this extension.
const WorkingPage = () => <p>topics page</p>;
const StubDetails = ({ object }: DetailsProps) => <ThrowsFor object={object} />;
const ThrowingMenuItem = (_props: MenuItemProps) => <Throws message="menu item" />;

describe("ErrorBoundary", () => {
  it("renders the error page in place of a child that throws, logs it, and leaves its siblings rendered", () => {
    render(
      <>
        <p>before</p>
        <ErrorBoundary extension={extension}>
          <p>inside</p>
          <Throws />
        </ErrorBoundary>
        <p>after</p>
      </>,
    );

    expect(screen.getByText("Error: boom")).toBeDefined();
    expect(screen.queryByText("inside")).toBeNull();
    expect(screen.getByText("before")).toBeDefined();
    expect(screen.getByText("after")).toBeDefined();
    expect(Common.logger.error).toHaveBeenCalledTimes(1);
    expect(Common.logger.error).toHaveBeenCalledWith("[kafka-extension]: Error: boom", expect.any(String));
  });

  it("renders nothing in place of a child that throws with the fallback nothing, and logs it", () => {
    render(
      <ul>
        <li>Edit</li>
        <ErrorBoundary extension={extension} fallback="nothing">
          <li>Suspend</li>
          <Throws message="menu item" />
        </ErrorBoundary>
        <li>Delete</li>
      </ul>,
    );

    expect(screen.getAllByRole("listitem").map((item) => item.textContent)).toEqual(["Edit", "Delete"]);
    expect(Common.logger.error).toHaveBeenCalledWith("[kafka-extension]: Error: menu item", expect.any(String));
  });

  it("renders its children without an error", () => {
    render(
      <ErrorBoundary extension={extension}>
        <p>inside</p>
      </ErrorBoundary>,
    );

    expect(screen.getByText("inside")).toBeDefined();
    expect(Common.logger.error).not.toHaveBeenCalled();
  });

  it("renders the children again for a new reset key", () => {
    const broken = objectOf(true);
    const fixed = objectOf(false);
    const { rerender } = render(
      <ErrorBoundary extension={extension} resetKey={broken}>
        <ThrowsFor object={broken} />
      </ErrorBoundary>,
    );

    expect(screen.getByText("Error: broken object")).toBeDefined();

    rerender(
      <ErrorBoundary extension={extension} resetKey={fixed}>
        <ThrowsFor object={fixed} />
      </ErrorBoundary>,
    );

    expect(screen.getByText("fine object")).toBeDefined();
    expect(screen.queryByText("Error: broken object")).toBeNull();
  });
});

describe("the registration wrappers", () => {
  it("keep an error of a page inside the page", () => {
    const { Page } = withPageErrorBoundary(extension, {
      id: "broken",
      components: { Page: () => <Throws /> },
    }).components;

    render(
      <>
        <Page params={{}} />
        <p>sidebar</p>
      </>,
    );

    expect(screen.getByText("Error: boom")).toBeDefined();
    expect(screen.getByText("sidebar")).toBeDefined();
    expect(Common.logger.error).toHaveBeenCalledTimes(1);
    expect(Common.logger.error).toHaveBeenCalledWith("[kafka-extension]: Error: boom", expect.any(String));
  });

  it("render a page that works as usual", () => {
    const { Page } = withPageErrorBoundary(extension, {
      id: "topics",
      components: { Page: WorkingPage },
    }).components;

    render(<Page params={{}} />);

    expect(screen.getByText("topics page")).toBeDefined();
    expect(Common.logger.error).not.toHaveBeenCalled();
  });

  it("keep an error of a details panel inside the panel, and render it again for a new object", () => {
    const { Details } = withDetailsErrorBoundary(extension, {
      kind: "KafkaTopic",
      apiVersions: ["kafka.strimzi.io/v1beta2"],
      components: { Details: StubDetails },
    }).components;

    const { rerender } = render(
      <>
        <Details object={objectOf(true)} />
        <p>other panel</p>
      </>,
    );

    expect(screen.getByText("Error: broken object")).toBeDefined();
    expect(screen.getByText("other panel")).toBeDefined();

    rerender(
      <>
        <Details object={objectOf(false)} />
        <p>other panel</p>
      </>,
    );

    expect(screen.getByText("fine object")).toBeDefined();
  });

  it("render nothing for a menu item that throws, and leave the other items rendered", () => {
    const { MenuItem } = withMenuItemErrorBoundary(extension, {
      kind: "KafkaTopic",
      apiVersions: ["kafka.strimzi.io/v1beta2"],
      components: { MenuItem: ThrowingMenuItem },
    }).components;

    render(
      <ul>
        <li>Edit</li>
        <MenuItem object={objectOf(false)} toolbar={false} />
        <li>Delete</li>
      </ul>,
    );

    expect(screen.getAllByRole("listitem").map((item) => item.textContent)).toEqual(["Edit", "Delete"]);
    expect(screen.queryByText(/menu item/)).toBeNull();
    expect(Common.logger.error).toHaveBeenCalledWith("[kafka-extension]: Error: menu item", expect.any(String));
  });
});
