# Review Process

This document describes guidelines and responsibilities for participants of
pull request reviews.

## Author Responsibilities

- **Describe the problem up front.**  In the PR description, include a concise,
  self-contained summary of the issue being solved and link to relevant specs,
  issues, or discussions.  For open-source PRs, capture the context in one clear
  section so reviewers don’t have to sift through comment threads.
- **Ship code with tests.**  Submit unit and integration tests alongside the
  implementation to demonstrate expected behavior and prevent regressions.
- **Instrument for production.**  Add meaningful tracing or logging so the
  changes can be monitored and debugged once deployed.
- **Keep PRs focused.**  Split large or mixed-purpose changes into smaller,
  logical units (e.g., separate refactors from feature work).
- **Highlight important changes.**  Add your own comments to the diffs to call
  out important changes that require extra scrutiny, or to explain a change
  whose inclusion isn't self-evident.
- **Choose your reviewers.**  The author should assign an initial set of
  required and optional reviewers.  Reviewers may change their role later after
  discussing with the author.
- **Any open issue blocks completion.**  Do not complete a review with open
  issues.  All issues must be resolved by the reviewer who created them, or
  another reviewer delegate.  Do not override tooling that prevents completion.
- **Do not resolve reviewer issues.**  Only reviewers should resolve reviewer
  issues.  Ideally the creator of the issue should resolve it, but reviewers may
  close each other's issues under certain circumstances.

## Reviewer Responsibilities

- **Understand what you’re approving.**  If anything is unclear, ask questions.
  Require in-code comments that explain why a solution exists - not just what it
  does.  PR comments are not a substitute for code comments.
- **Read the full PR description.**  Context matters; don’t skip it.
- **Review the changes, not the design.**  A code review is not the time to
  review a design choice.  If you think an alternative design would be better,
  discuss with the author and team, and ask for a new PR if the new design is
  implemented instead.
- **Demand tests.**  If appropriate tests are missing, request them.  When
  testing truly isn’t feasible, insist on a documented rationale.
- **Note partial reviews.**  If you reviewed only part of the code, say so in
  the PR.
- **Delegate wisely.**  If you cannot complete your reviewer role's
  responsibilities, delegate to another subject-matter expert.
- **Own your approval.**  You are accountable for the quality and correctness of
  the code you sign off on.
- **Block completion on all open issues.**  Any issue you open against a review
  must be resolved to your satisfaction before a review may complete.
  Resolution may include new code changes to fix a problem, new in-code
  documentation to explain something, or a discussion within the PR itself.
- **Do not close other reviewer's issues.** The reviewer who created an issue
  should be the one to resolve it.  Exceptions include explicit delegation or
  OOTO absences.  Any delegations should be discussed with the team.
- **Never rubber-stamp.**  Trust the author but verify the code - always conduct
  a real review.
- **Review in a timely manner.**  Reviewers should aim to perform a review
  within 2 business days of receiving the initial request, and 1 business day
  for follow-up changes.  Reviewing a PR is higher priority than most other
  tasks.

## Backwards Compatibility Awareness

Subtle changes may have backwards compatibility implications.  Below are some of
the code changes and side-effects to watch out for:

- Do these changes force updates to existing integration tests - a sign they may
  introduce a breaking change?
- Will these changes modify the public API’s behavior in a way that could break
  existing applications when they upgrade to the new binaries?
- For any non-security, breaking changes that impact customers, have we provided
  an opt-out - such as a connection-string flag, AppContext switch, or similar
  setting - that lets users revert to the previous behavior?