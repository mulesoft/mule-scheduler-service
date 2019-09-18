def UPSTREAM_PROJECTS_LIST = [ "Mule-runtime/mule/mule-4.2.2" ]

Map pipelineParams = [ "upstreamProjects" : UPSTREAM_PROJECTS_LIST.join(','),
                       "mavenSettingsXmlId" : "mule-runtime-maven-settings-MuleSettings",
                       "archiveArtifacts" : "**/hs_*.log" ]

runtimeProjectsBuild(pipelineParams)
