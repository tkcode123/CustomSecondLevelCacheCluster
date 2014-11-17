# Load MSBuild assembly if it's not loaded yet.
Add-Type -AssemblyName 'Microsoft.Build, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a'

# Grab the loaded MSBuild project for the project
$msbuild = [Microsoft.Build.Evaluation.ProjectCollection]::GlobalProjectCollection.GetLoadedProjects($project.FullName) | Select-Object -First 1

# Remove the EnhancerAssembly project setting if exist
$msbuild.Xml.Properties | Where-Object {$_.Name.ToLowerInvariant() -eq "enhancerassembly" } | Foreach { 
    $_.Parent.RemoveChild( $_ ) 
}

# Remove included OpenAccess.targets if any
$msbuild.Xml.Imports | Where-Object {$_.Project.ToLowerInvariant().EndsWith("openaccess.targets") } | Foreach { 
    $_.Parent.RemoveChild( $_ ) 
}

# Remove included OpenAccessNuget.targets if any
$msbuild.Xml.Imports | Where-Object {$_.Project.ToLowerInvariant().EndsWith("openaccessnuget.targets") } | Foreach { 
    $_.Parent.RemoveChild( $_ ) 
}