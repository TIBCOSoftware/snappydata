properties([
  parameters([
      string(defaultValue: 'product', description: '', name: 'buildtarget', trim: false),
      string(defaultValue: 'anonymous', description: '', name: 'username', trim: false),
      string(defaultValue: 'master', description: '', name: 'snappybranch', trim: false),
      string(defaultValue: 'snappy/branch-2.1', description: 'spark repository branch name.', name: 'sparkbranch', trim: false),
      string(defaultValue: 'snappy/master', description: 'store repository branch name. ', name: 'snappystorebranch', trim: false),
      string(defaultValue: 'snappydata', description: 'store repository branch name. ', name: 'sparkjobserverbranch', trim: false)
  ])
])

node {

		stage('Checkout'){
    	checkout([
    		$class: 'GitSCM',
    		branches: [[name: "${snappybranch}"]],
    		doGenerateSubmoduleConfigurations: false,
    		extensions: [],
    		submoduleCfg: [],
    		userRemoteConfigs: [
    			[url: 'https://github.com/SnappyDataInc/snappydata.git']
    		]
    	])

    	parallel (
    		"checkout spark": {
    			dir('spark') {
    				checkout([
    					$class: 'GitSCM',
    					branches: [[name: "${sparkbranch}"]],
    					doGenerateSubmoduleConfigurations: false,
    					extensions: [],
    					submoduleCfg: [],
    					userRemoteConfigs: [
    						[url: 'https://github.com/SnappyDataInc/spark.git']
    					]
    				])
    			}
    		},

    		"checkout spark-jobserver": {
    			dir('spark-jobserver') {
    				checkout([
    					$class: 'GitSCM',
    					branches: [[name: "${sparkjobserverbranch}"]],
    					doGenerateSubmoduleConfigurations: false,
    					extensions: [],
    					submoduleCfg: [],
    					userRemoteConfigs: [
    						[url: 'https://github.com/SnappyDataInc/spark-jobserver.git']
    					]
    				])
    			}
    		},

    		"checkout snappy-store": {
    			dir('store') {
    				checkout([
    					$class: 'GitSCM',
    					branches: [[name: "${snappystorebranch}"]],
    					doGenerateSubmoduleConfigurations: false,
    					extensions: [],
    					submoduleCfg: [],
    					userRemoteConfigs: [
    						[url: 'https://github.com/SnappyDataInc/snappy-store.git']
    					]
    				])
    			}
    		}
    	)
    }

    stage('Build'){
        script {
            sh './gradlew cleanAll'
        }
    }

    stage('Test'){
        script {
            sh './gradlew ${buildtarget}'
        }
    }

    stage('Copy'){
        fileOperations([folderCopyOperation(destinationFolderPath: '/home/ubuntu/build-backups/', sourceFolderPath: '/var/lib/jenkins/workspace/tibco-computedb-ci/build-artifacts')])
    }
}

