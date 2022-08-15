import com.homeaway.devtools.jenkins.testing.JenkinsPipelineSpecification

class BuildJavascriptAppSpec extends JenkinsPipelineSpecification {
    def buildJavascriptApp = null

    def setup() {
        buildJavascriptApp = loadPipelineScriptForTest("vars/buildJavascriptApp.groovy")
    }

    def "[buildJavascriptApp] will run npm publish if deploy is true"() {
        when:
            buildJavascriptApp deploy: true
        then:
            1 * getPipelineMock("sh")("npm publish")
    }  
}
