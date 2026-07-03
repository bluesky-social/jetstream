# Contributing

Jetstream accepts contributions, but maintainers prioritize high-quality issues and pull requests that are small, well-scoped, and discussed before significant implementation work starts.

For high-level atproto discussion, design questions, and community support, use the [atproto discussion forum](https://github.com/bluesky-social/atproto/discussions). Do not open a Jetstream issue for general protocol support.

## Rules

- We may not respond to every issue or pull request.
- We may close an issue or pull request without extensive feedback.
- We may lock discussions or contributions if maintainer attention is being overloaded.
- We do not provide general support for local build or environment issues.

## Issues

Before filing an issue, search existing issues and confirm the report is about Jetstream itself. Use issues for bugs, concrete feature requests, and small maintenance tasks.

A good issue includes:

- Context: what is wrong or missing, and why it matters.
- Definition of done: the observable behavior, test, metric, or document change that would close the issue.
- Notes: relevant alternatives, constraints, or follow-up work that should not be included in the current change.

## Pull Requests

Open an issue and allow time for discussion before submitting substantial changes. Pull requests are easiest to review when they solve one problem, keep commits focused, and include the right tests or documentation updates.

Avoid pull requests that:

- Refactor large parts of the codebase without prior discussion.
- Add entirely new features without prior discussion.
- Change the tooling, workflows, or frameworks used without prior discussion.
- Introduce unnecessary dependencies.

## Development

Jetstream uses Nix for its pinned Go and toolchain environment. Start with:

```sh
./dev.sh
# or
just dev
```

Common checks:

```sh
just        # lint and short tests
just lint
just test
just test-race
just oracle
```

Use the `justfile` recipes instead of calling tools directly where possible. They are the interface CI uses and keep local behavior aligned with the repo.

## Security

Do not report possible security vulnerabilities in public GitHub issues or discussion threads. Follow [SECURITY.md](SECURITY.md) instead.

## CI for External Contributions

CI intentionally runs on `push` events only. It does not run on fork pull requests, because that would execute untrusted contributor code in GitHub Actions.

For an external contribution, a maintainer must push the branch or commits to this repository before CI runs. If your pull request does not show CI results, that does not necessarily mean CI is broken.

Maintainers should preserve this security posture unless the workflow threat model is updated in the same change.
