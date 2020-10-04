# stale-branch-scanner
scans repositories for stale branches

# Quickstart

```
python scan_unmerged_branches.py [BRANCH] [REPO_DIR] [options]

if no BRANCH is provided, uses the default main branch (main)
if no REPO_DIR is provided, uses . (current working directory)

if no --output path is provided, print to STDOUT

if --input file_path is provided, do not read BRANCH or REPO_DIR from arguments,
instead use input file_path as configuration (.json, .csv, or .txt file allowed)
Input file Modes:
    .json :
        file is json file with an array/list as root object. each item in the list is a config
        config MUST have branch and repo_dir values, and can define additional supported options 
    .csv :
        file is csv file with a header row. each additional row is a config
        config MUST have "branch" and "repo_dir" columns, and can define additional supported options
        rows that do not define an option should leave it blank (empty string)
    .txt :
        file is a txt file. each line is a a config
        config is BRANCH and REPO_DIR separated by whitespace, can define no additional options
        
    supported options (json and csv mode only): include_main, stale, fetch_first
```

# Future

 1) Specify a repo as a url and automatically download the repo.
 
    for example; specifying a github url and it will download that repo and then scan it.
    
 2) Enhance the code with docstrings
 3) refactor the code to make it more understandable and robust 

# Contact / Support
creator: [Inbar Rose](https://github.com/InbarRose)
