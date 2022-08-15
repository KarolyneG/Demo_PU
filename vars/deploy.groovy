def call() {
        if (config.deploy) {
                sh "npm publish"
            }
}
