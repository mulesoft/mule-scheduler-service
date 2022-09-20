def UPSTREAM_PROJECTS_LIST = [ "Mule-runtime/mule/4.3.0-OCTOBER-2022" ]

Map pipelineParams = [ "upstreamProjects" : UPSTREAM_PROJECTS_LIST.join(','),
                       "mavenSettingsXmlId" : "mule-runtime-maven-settings-MuleSettings",
                       "archiveArtifacts" : "**/hs_*.log",
                       "projectType" : "Runtime" ]

runtimeBuild(pipelineParams)
