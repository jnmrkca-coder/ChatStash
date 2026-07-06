# ChatStash

ChatStash is a self-hosted ChatGPT export organizer. It runs locally, indexes extracted exports or zip exports into SQLite, and lets you search, curate, bulk tag, bulk rename, and export selected conversations without modifying the original backup files.

## Run

```powershell
python .\chatstash.py
```

Open:

```text
http://127.0.0.1:8765
```

On first run, create an admin account. Passwords are stored as PBKDF2 hashes in `instance/chatstash.sqlite3`; plaintext passwords are never written to app files. There is no password recovery flow.

## Import Model

ChatStash recognizes ChatGPT exports by marker files:

- Extracted folder: `export_manifest.json` plus `conversations*.json`
- Zip file: contains `export_manifest.json` plus `conversations*.json`

The default library path is the project folder. The app can scan manually or poll watched library paths. It imports each conversation into SQLite while preserving:

- Original source path
- Original inner JSON chunk path
- Raw conversation JSON
- Normalized text content
- Native timestamps, model, archive/star/read-only/study flags
- Derived mode, message counts, code block count, URL count, attachment count

App-side curation metadata is separate:

- Custom title
- Tags
- Project
- Rating
- Custom fields

## Search

The search box uses SQLite FTS. Useful patterns:

```text
"exact phrase"
design AND css
project OR invoice
python -draft
raw:title NEAR export
```

Filters can be combined with the search box:

- Model
- Mode
- Project
- Tag
- Rating
- Date range
- Has code
- Has attachments
- Starred
- Archived

## Bulk Actions

Select conversations, then use the bottom action bar:

- Add tags
- Replace tags
- Set project
- Set rating
- Rename custom title with placeholders

Rename placeholders:

```text
{counter}
{id}
{date}
{title}
{project}
{model}
{mode}
{tag0}
```

Example:

```text
{date} - {project} - {counter} - {title}
```

## Exports

Selected conversations can be exported as:

- Consolidated Markdown
- Zip of per-conversation JSON files
- Zip of per-conversation Markdown files
- Metadata CSV
- Bundle zip with Markdown, JSON, manifest, and metadata CSV

Generated filenames use the same placeholder pattern as bulk rename. Original backup files are not renamed or edited.
