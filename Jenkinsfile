@Library('Demo_PU')_
node {
  git url: "https://github.com/werne2j/sample-nodejs"


stage('Install') {
  sh "npm install"
}

stage("Test") {
  
  sh "mvn clean test"
 
}
  
 stage("Deploy") {
    deploy()
        }
}
