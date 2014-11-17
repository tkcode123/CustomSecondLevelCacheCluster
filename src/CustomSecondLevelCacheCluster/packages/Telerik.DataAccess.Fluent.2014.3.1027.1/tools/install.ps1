param($installPath, $toolsPath, $package, $project)
	
	. (Join-Path $toolsPath "cleanProject.ps1")

#Add the proper EnhancerAssembly project setting
$enhancerFile = [System.IO.Path]::Combine($toolsPath, 'enhancer\enhancer.exe')
$enhancerUri = new-object Uri($enhancerFile)
$solutionUri = new-object Uri($project.DTE.Solution.FullName)
$enhancerRelativeUri = $solutionUri.MakeRelativeUri($enhancerUri)
$enhancerRelativePath = $enhancerRelativeUri.ToString().Replace([System.IO.Path]::AltDirectorySeparatorChar, [System.IO.Path]::DirectorySeparatorChar)
if($enhancerRelativeUri.IsAbsoluteUri)
{
	# The Enhancer path is Absolute
	if($enhancerRelativePath.StartsWith("file:"))
	{
		# Avoid getting "file:file:" for files on Shared Drive
		$enhancerRelativePath = $enhancerRelativePath.Substring(5);
	}

	# We don't need to concat the Absolute path with the SolutionDir
	$msbuild.Xml.AddProperty('EnhancerAssembly', $enhancerRelativePath) | out-null
}
else
{
	# The NuGet repository is Relative to the Solution so we need to concat it with the SolutionDir.
	$msbuild.Xml.AddProperty('EnhancerAssembly','$(SolutionDir)\' + $enhancerRelativePath) | out-null
}

# Include the new OpenAccess targets right after the CSharp/VisualBasic targets in order to be before the
# NuGet targets ensuring that the packages restore will be executed before the Enhancement
$openAccessTargetsImport = $msbuild.Xml.CreateImportElement('OpenAccessNuget.targets');
$msTargetsImport = $null
if($project.Type -eq "C#")
{
	$msTargetsImport = $msbuild.Xml.Imports | Where-Object { $_.Project.EndsWith("Microsoft.CSharp.targets") }
}
elseif($project.Type -eq "VB.NET")
{
	$msTargetsImport = $msbuild.Xml.Imports | Where-Object { $_.Project.EndsWith("Microsoft.VisualBasic.targets") }
}

if($msTargetsImport -ne $null)
{
	$msbuild.Xml.InsertAfterChild($openAccessTargetsImport, $msTargetsImport)
}
else
{
	$msbuild.Xml.AddImport($openAccessTargetsImport) 
}

# Save the project
$project.Save()