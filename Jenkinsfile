node {
    try {

    emailext attachLog: true, body: "Build : # $BUILD_NUMBER. \n\n Log location : /var/logs/jenkins/log/oss/$BUILD_NUMBER \n Scheduled By : $username \n Branch Information : snappydata - ${snappybranch}, Spark - ${sparkbranch}, spark-jobserver - ${sparkjobserverbranch}, snappy-store - ${snappystorebranch} ",
		compressLog: true, recipientProviders: [developers()],
		subject: '[Jenkins] Build # $BUILD_NUMBER is Started', to: '$useremail'

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
			fileOperations([folderCopyOperation(destinationFolderPath: '/var/log/jenkins/log/oss/$BUILD_NUMBER', sourceFolderPath: '/var/lib/jenkins/workspace/$PROJECT_NAME/build-artifacts')])
		}
    } finally {
	    emailext attachLog: true, body: ' Project Name : $PROJECT_NAME \n Build : # $BUILD_NUMBER. \n Log location : /var/logs/jenkins/log/oss/$BUILD_NUMBER \n Scheduled By : $username      \n Brach Information : snappydata - ${snappybranch}, Spark - ${sparkbranch}, spark-jobserver - ${sparkjobserverbranch}, snappy-store - ${snappystorebranch}  \n Check console output at $BUILD_URL to view the results.',
	    compressLog: true, recipientProviders: [developers()],
		subject: '[Jenkins] $PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!', to: '$useremail'
    }

}

