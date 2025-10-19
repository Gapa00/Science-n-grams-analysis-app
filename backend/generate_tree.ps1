$exclude = @("node_modules", ".git", "__pycache__", "dist", "build", ".venv", "env", "venv")
$projectRoot = Get-Location

Get-ChildItem -Recurse -Force |
    Where-Object {
        # Get each folder name in the full path
        $pathParts = $_.FullName.Substring($projectRoot.Path.Length + 1).Split("\")
        # Check if any folder in the path is in the exclude list
        -not ($pathParts | Where-Object { $exclude -contains $_ })
    } |
    ForEach-Object {
        $_.FullName.Substring($projectRoot.Path.Length + 1)
    } |
    Out-File -Encoding UTF8 project_structure.txt
