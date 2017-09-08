/*!
 * st-amq
 * Copyright(c) 2017 zhennanzhuce
 * MIT Licensed
 */
'use strict';

var gulp = require("gulp");
var babel = require("gulp-babel");
var clean = require('gulp-clean');
var uglify = require('gulp-uglify');

gulp.task("clean", () => {
  return gulp.src("dist/*").pipe(clean({ force: true }));
});

gulp.task("default", ['copy1', 'copy2', 'copy3'], () => {
  return gulp.src("src/**/*.js")
             .pipe(babel())
             .pipe(uglify({ mangle: { toplevel: true } }))
             .pipe(gulp.dest("dist"));
});

gulp.task("copy1", () => {
  return gulp.src("src/package.json")
             .pipe(gulp.dest("dist"));
});

gulp.task("copy2", () => {
  return gulp.src("src/LICENSE")
             .pipe(gulp.dest("dist"));
});

gulp.task("copy3", () => {
  return gulp.src("src/README.md")
             .pipe(gulp.dest("dist"));
});