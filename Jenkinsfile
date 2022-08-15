@Library('Demo_PU')_
node {
  git url: "https://github.com/werne2j/sample-nodejs"


stage('Install') {
  
  env.NODEJS_HOME = "${tool 'node'}"
  env.PATH = "${env.NODEJS_HOME}/bin:${env.PATH}"
  sh "npm install"
}

stage("Test") {
  
  sh "mvn clean test"
 
}
  
 stage("Deploy") {
    deploy()
        }
}
