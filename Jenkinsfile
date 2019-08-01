node {
    try {

        emailext attachLog: true, body: '$PROJECT_NAME - Build Reference # $BUILD_NUMBER. Build logs can be find at /var/logs/jenkins/log/oss/$BUILD_NUMBER ',
		    compressLog: true, recipientProviders: [developers()],
		    subject: '$PROJECT_NAME - Build Ref. Number : $BUILD_NUMBER - Status : Started! - User : ${jenkinsUserName}', to: '${useremail}'

		stage('Checkout') {
			checkout([
				$class: 'GitSCM',
				branches: [
					[name: "${snappybranch}"]
				],
				doGenerateSubmoduleConfigurations: false,
				extensions: [],
				submoduleCfg: [],
				userRemoteConfigs: [
					[url: 'https://github.com/SnappyDataInc/snappydata.git']
				]
			])

			parallel(
				"checkout spark": {
					dir('spark') {
						checkout([
							$class: 'GitSCM',
							branches: [
								[name: "${sparkbranch}"]
							],
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
							branches: [
								[name: "${sparkjobserverbranch}"]
							],
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
							branches: [
								[name: "${snappystorebranch}"]
							],
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

		stage('Build') {
			script {
				sh "./gradlew cleanAll"
			}
		}

		stage('Test') {
			script {
				sh "./gradlew ${target}"
			}
		}

		stage('Copy') {
			fileOperations([folderCreateOperation('/var/log/jenkins/log/oss/$BUILD_NUMBER')])
			fileOperations([folderCopyOperation(destinationFolderPath: '/var/log/jenkins/log/oss/$BUILD_NUMBER', sourceFolderPath: '/var/lib/jenkins/workspace/tibco-computedb-ci/build-artifacts')])
		}
    } finally {
	    emailext attachLog: true, body: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS: \n Check console output at $BUILD_URL to view the results.',
		compressLog: true, recipientProviders: [developers()],
		subject: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!', to: '${useremail}'
    }

}

