def UPSTREAM_PROJECTS_LIST = [ "Mule-runtime/mule/support/4.1.1" ]

Map pipelineParams = [ "upstreamProjects" : UPSTREAM_PROJECTS_LIST.join(','),
                       "mavenSettingsXmlId" : "mule-runtime-maven-settings-MuleSettings",
                       "archiveArtifacts" : "**/hs_*.log",
                       "projectType" : "Runtime" ]

runtimeBuild(pipelineParams)
