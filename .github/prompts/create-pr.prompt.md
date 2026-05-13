---
description: "Create a well-structured PR for mssql-jdbc"
name: "mssql-jdbc-pr"
agent: 'agent'
---
# Create Pull Request Prompt for microsoft/mssql-jdbc

You are a development assistant helping create a pull request for the Microsoft JDBC Driver for SQL Server.

## PREREQUISITES

Before creating a PR, ensure:
1. All tests pass
2. Code changes are complete and working
3. Build compiles cleanly (use `#build`)
4. Code follows project formatting (`mssql-jdbc_formatter.xml`)

---

## TASK

Help the developer create a well-structured pull request. Follow this process sequentially.

**Use GitHub MCP tools** (`mcp_github_*`) for PR creation when available.

---

## STEP 1: Verify Current Branch State

### 1.1 Check Current Branch

```bash
git branch --show-current
```

**If on `main`:**
> You're on the main branch. You need to create a feature branch first.
> Continue to Step 2.

**If on a feature branch:**
> You're on a feature branch. Skip to Step 3.

### 1.2 Check for Uncommitted Changes

```bash
git status
```

**If there are uncommitted changes**, they need to be committed before creating a PR.

---

## STEP 2: Create Feature Branch (If on main)

### 2.1 Ensure main is Up-to-Date

```bash
git checkout main
git pull origin main
```

### 2.2 Create and Switch to Feature Branch

**Branch Naming Convention:** `users/<alias>/<description>` or `users/<alias>/<type>/<description>`

| Type | Use For | Example |
|------|---------|---------|
| `feat` | New features | `users/muskgupta/feat/connection-timeout` |
| `fix` | Bug fixes | `users/muskgupta/fix/bulk-copy-null-handling` |
| `doc` | Documentation | `users/muskgupta/doc/update-readme` |
| `refactor` | Refactoring | `users/muskgupta/refactor/simplify-tds-parser` |
| `chore` | Maintenance | `users/muskgupta/chore/update-deps` |
| (no type) | General work | `users/muskgupta/jdk26` |

Ask the developer for their alias and branch purpose, then:

```bash
git checkout -b users/<alias>/<type>/<description>
```

---

## STEP 3: Review Changes

### 3.1 Check What's Changed

```bash
# See all changed files
git status

# See detailed diff
git diff

# See diff for staged files
git diff --staged
```

### 3.2 Verify Changes are Complete

Ask the developer:
> "Are all your changes ready to be committed? Do you need to make any additional modifications?"

---

## STEP 4: Stage and Commit Changes

### 4.1 Stage Changes

> **Prefer staging specific files** over `git add .`

```bash
# Stage specific files
git add <file1> <file2> <folder/>

# Check what's staged
git status
```

**Files to typically EXCLUDE from commits:**
- `target/` - Build output
- `*.class` - Compiled classes
- `.idea/`, `.settings/`, `.project` - IDE files
- `*.iml` - IntelliJ module files

### 4.2 Create Commit Message

```bash
git commit -m "<type>: <description>

<detailed description if needed>"
```

**Commit message types:** `feat`, `fix`, `doc`, `refactor`, `chore`, `test`, `style`

**Examples:**
```bash
git commit -m "feat: add support for JDK 26 compilation

- Added jre26 Maven profile
- Updated byte-buddy to version compatible with JDK 26
- Updated Mockito to version supporting JDK 26"
```

---

## STEP 5: Push Branch

```bash
# Push branch to remote (first time)
git push -u origin <branch-name>

# Subsequent pushes
git push
```

---

## STEP 6: Create Pull Request

> **MANDATORY:** Before creating a PR, you MUST confirm **3 things** with the developer:
> 1. **PR Title** - Suggest options, get approval
> 2. **Issue Link** - Ask for GitHub issue or ADO work item
> 3. **PR Description** - Show full description, get approval

---

### 6.1 PR Title

Suggest 3-5 title options to the developer based on the changes:

**Example:**
```
Here are some title options for your PR:

1. Add JDK 26 support to mssql-jdbc
2. feat: Enable compilation and testing with JDK 26
3. Add jre26 Maven profile for JDK 26 compatibility

Which one do you prefer, or would you like to modify one?
```

---

### 6.2 Issue Link

> **NEVER auto-add an issue number.** Ask the developer explicitly.

**Process:**
1. Search GitHub issues for potentially related issues
2. If found similar ones, list them as **suggestions only**
3. Ask: "Which issue or ADO work item should this PR be linked to?"
4. User can provide: GitHub issue, ADO work item, both, or none

---

### 6.3 PR Description (Use PR Template)

> Show the full PR description to the developer and get approval before creating the PR.

**Use the project's PR template format** (from `.github/PULL_REQUEST_TEMPLATE/pr-template.md`):

```markdown
## Description

<Summary of the changes being introduced>

- <Change 1>
- <Change 2>
- <Change 3>

## Issues

<Link to GitHub issue or ADO work item>

## Testing

<Description of automated tests created or modified>
<Manual testing steps performed>

## Guidelines

Please review the contribution guidelines before submitting a pull request:

- [Contributing](/CONTRIBUTING.md)
- [Code of Conduct](/CODE_OF_CONDUCT.md)
- [Best Practices](/coding-best-practices.md)
- [Coding Guidelines](/Coding_Guidelines.md)
- [Review Process](/review-process.md)
```

**Do not use unicode characters or superlatives in the PR description.** Focus on functional changes and reasoning.

---

### 6.4 Create PR via GitHub MCP (Preferred)

Use the `mcp_github_create_pull_request` tool:

```
Owner: microsoft
Repo: mssql-jdbc
Title: <description>
Head: <your-branch-name>
Base: main
Body: <PR description>
```

### 6.5 Alternative: Create PR via GitHub CLI

If MCP is not available:

```bash
gh pr create \
  --title "<description>" \
  --body "## Description

<summary>

## Issues

<link related GitHub issue and/or ADO work item if applicable>

## Testing

<testing details>

## Guidelines

- [Contributing](/CONTRIBUTING.md)
- [Code of Conduct](/CODE_OF_CONDUCT.md)
- [Best Practices](/coding-best-practices.md)
- [Coding Guidelines](/Coding_Guidelines.md)
- [Review Process](/review-process.md)" \
  --base main
```

### 6.6 Alternative: Create PR via Web

```bash
# Get the URL to create PR
echo "https://github.com/microsoft/mssql-jdbc/compare/main...<branch-name>?expand=1"
```

---

## STEP 7: PR Checklist

Before submitting, verify:

```markdown
## PR Checklist

- [ ] PR description has a clear summary of changes
- [ ] PR links to a GitHub issue or ADO work item
- [ ] Branch is based on latest `main`
- [ ] Build compiles cleanly (`mvn clean compile -Pjre11`)
- [ ] All tests pass locally
- [ ] Code follows project formatting (`mssql-jdbc_formatter.xml`)
- [ ] No sensitive data (passwords, keys, connection strings) in code
- [ ] No IDE files or build artifacts committed
- [ ] Javadoc updated for new/changed public APIs
- [ ] Error messages added to `SQLServerResource.java` if needed
- [ ] Review process guidelines followed (/review-process.md)
```

---

## Troubleshooting

### "Updates were rejected because the remote contains work..."

**Cause:** Remote has commits you don't have locally.

**Fix:**
```bash
git pull origin main --rebase
git push
```

### "Permission denied" when pushing

**Cause:** SSH key or token not configured.

**Fix:**
```bash
# Check remote URL
git remote -v

# If using HTTPS, ensure you have a token
# If using SSH, ensure your key is added to GitHub
```

### Merge conflicts with main

**Cause:** main has changed since you branched.

**Fix:**
```bash
# Update main
git checkout main
git pull origin main

# Rebase your branch
git checkout <your-branch>
git rebase main

# Resolve conflicts if any, then
git push --force-with-lease
```

### Accidentally committed to main

**Fix:**
```bash
# Create a branch from current state
git branch <new-branch-name>

# Reset main to match remote
git checkout main
git reset --hard origin/main

# Switch to your branch
git checkout <new-branch-name>
```

### Need to update PR with more changes

**Fix:**
```bash
# Make your changes
git add <files>
git commit -m "fix: address PR feedback"
git push

# PR automatically updates
```

### PR has too many commits, want to squash

**Fix:**
```bash
# Interactive rebase to squash commits
git rebase -i HEAD~<number-of-commits>

# Change 'pick' to 'squash' for commits to combine
# Save and edit commit message
git push --force-with-lease
```

---

## Quick Reference

### Branch Naming

`users/<alias>/<type>/<description>` or `users/<alias>/<description>`

### Common Git Commands for PRs

```bash
# Check current state
git status
git branch --show-current
git log --oneline -5

# Create and switch branch
git checkout -b users/<alias>/feat/my-feature

# Stage and commit
git add <files>
git commit -m "feat: description"

# Push
git push -u origin <branch-name>

# View PR status (gh CLI)
gh pr status
gh pr view
```

---

## After PR is Created

1. **Monitor CI** - Watch for build and test failures
2. **Respond to reviews** - Address reviewer comments promptly
3. **Keep branch updated** - Rebase if main changes significantly
4. **Follow review process** - See `/review-process.md` for guidelines
5. **Merge** - Once approved, merge via GitHub (usually squash merge)
