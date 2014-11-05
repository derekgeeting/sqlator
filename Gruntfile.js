module.exports = function(grunt) {
	grunt.initConfig({
		watch: {
			files: ['**'],
			tasks: ['nodemon']
		},
		nodemon: {
			dev: {
				script: 'sqlator.js'
			}
		}
	})
	grunt.loadNpmTasks('grunt-contrib-watch');
	grunt.loadNpmTasks('grunt-nodemon');

	grunt.registerTask('default', ['nodemon'])
};