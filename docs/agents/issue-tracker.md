# Issue tracker: GitHub

Issues and specs live in Monsterwi/bakemono GitHub Issues.
Use the gh CLI from this repository.

- Create: gh issue create --title "..." --body-file <file>
- Read: gh issue view <number> --json number,title,body,labels,comments
- List: gh issue list --state open --json number,title,labels
- Comment: gh issue comment <number> --body-file <file>
- Labels: gh issue edit <number> --add-label "..." or --remove-label "..."
- Close: gh issue close <number>

Use GitHub native sub-issues and dependencies when available.
Otherwise record "Part of #<parent>" and "Blocked by: #<issue>" in the body.

“Publish to the issue tracker” means create a GitHub issue.
“Fetch the relevant ticket” means read the referenced GitHub issue.

## Pull requests as a triage surface

PRs as a request surface: no.
