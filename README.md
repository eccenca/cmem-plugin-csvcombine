# cmem-plugin-csvcombine

This eccenca Corporate Memory workflow plugin combines CSV files of the same structure identified by a regular expression into one dataset. It can be used as an alternative to using a zip archive of the files. The plugin checks the column headers for consistency, ignoring leading or trailing spaces.

## Parameters:

- Delimiter: The character used to separate values (default: ',')
- Quotechar: The character used to surround fields that contain the delimiter character (default: '"')
- Regex: Regular expression used to identify CSV filenames

## Development

- Run [task](https://taskfile.dev/) to see all major development tasks
- Use [pre-commit](https://pre-commit.com/) to avoid errors before commit
- This repository was created with [this]() [copier](https://copier.readthedocs.io/) template.

