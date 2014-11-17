param($installPath, $toolsPath, $package, $project)

	. (Join-Path $toolsPath "cleanProject.ps1")

$project.Save()
