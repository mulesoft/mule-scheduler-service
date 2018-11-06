def UPSTREAM_PROJECTS_LIST = [ "Mule-runtime/mule/mule-4.x" ]

Map pipelineParams = [ "upstreamProjects" : UPSTREAM_PROJECTS_LIST.join(','),
                       "mavenSettingsXmlId" : "mule-runtime-maven-settings-MuleSettings" ]

runtimeProjectsBuild(pipelineParams)
