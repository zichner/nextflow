/*
 * Copyright (c) 2013-2017, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2017, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.cli

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.text.SimpleDateFormat

import com.beust.jcommander.Parameter
import nextflow.Session
import nextflow.config.ConfigBuilder
import nextflow.exception.AbortOperationException
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import nextflow.file.FilePatternSplitter
import nextflow.util.MemoryUnit

/**
 * Implements `fs` command
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdFs extends CmdBase implements UsageAware {

    static final public NAME = 'fs'

    static final List<SubCmd> commands = new ArrayList<>()

    static {
        commands << new CmdCopy()
        commands << new CmdMove()
        commands << new CmdList()
        commands << new CmdCat()
        commands << new CmdRemove()
        commands << new CmdMakeDir()
    }

    abstract static class SubCmd<T> {

        abstract int getArity()

        abstract String getName()

        abstract String getDescription()

        abstract void apply(Path source, BasicFileAttributes attrs, Path target)

        String usage() {
            "Usage: nextflow fs ${name} " + (arity==1 ? "<path>" : "source_file target_file")
        }

        void execute(List<String> args) {
            if( args.size() < arity ) {
                throw new AbortOperationException(usage())
            }

            if( !args[0] ) {
                throw new AbortOperationException("Missing source file")
            }

            Path target = args.size()>1 ? args[1] as Path : null
            traverse(source(args[0])) { Path path, BasicFileAttributes attrs -> apply(path, attrs, target) }
        }

        protected String source(String path) {
            return path
        }

        protected void traverse( String source, Closure op ) {

            // if it isn't a glob pattern simply return it a normalized absolute Path object
            def splitter = FilePatternSplitter.glob().parse(source)
            if( splitter.isPattern() ) {
                final folder = splitter.folder as Path
                final pattern = splitter.fileName

                def opts = [:]
                opts.type = 'any'
                opts.relative = true

                FileHelper.visitFiles(opts, folder, pattern, op)
            }
            else {
                def normalised = splitter.strip(source)
                op.call(FileHelper.asPath(normalised), null)
            }

        }
    }

    static class CmdCopy extends SubCmd {

        @Override
        int getArity() { 2 }

        @Override
        String getName() { 'cp' }

        String getDescription() { 'Copy a file' }

        @Override
        void apply(Path source, BasicFileAttributes attrs, Path target) {
            FilesEx.copyTo(source, target)
        }

    }

    static class CmdMove extends SubCmd {

        @Override
        int getArity() { 2 }

        @Override
        String getName() { 'mv' }

        String getDescription() { 'Move a file' }

        @Override
        void apply(Path source, BasicFileAttributes attrs, Path target) {
            FilesEx.moveTo(source, target)
        }

    }

    static class CmdList extends SubCmd {

        static final FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        static final int LEN_DATE = 20

        static final int LEN_SIZE = 8

        @Override
        int getArity() { 1 }

        String getDescription() { 'List the content of a folder' }

        @Override
        String getName() { 'ls' }

        @Override
        void apply(Path source, BasicFileAttributes attr, Path unused ) {
            if( attr == null )
                attr = Files.readAttributes(source,BasicFileAttributes)
            
            if( attr.directory )
                println formatDir(source)
            else
                println formatFile(source,attr)
        }

        private formatDir(Path source) {
            def result = new StringBuilder()
            result << ''.padLeft(LEN_DATE+1)
            result << ''.padLeft(LEN_SIZE+1)
            result << (source.toString() + '/')
            result.toString()
        }

        private formatFile(Path source, BasicFileAttributes attrs)  {
            "${date(attrs.lastModifiedTime())} ${size(attrs.size())} ${source}"
        }

        private String date(FileTime time) {
            def millis = time?.toMillis()
            def result = millis ? FORMATTER.format(millis) : '-'
            result.padRight(LEN_DATE)
        }

        private String size(Long value) {
            def result = value != null ? new MemoryUnit(value).toString() : '-'
            result.padLeft(LEN_SIZE)
        }

        protected String source(String path) {
            def p = path as Path
            if( p.isDirectory() )
                FileHelper.toQualifiedPathString( p.resolve('*') )
            else
                path
        }
    }

    static class CmdCat extends SubCmd {

        @Override
        int getArity() { 1 }

        @Override
        String getName() { 'cat' }

        @Override
        String getDescription() { 'Print a file to the stdout' }

        @Override
        void apply(Path source, BasicFileAttributes attrs, Path target) {
            String line
            def reader = Files.newBufferedReader(source, Charset.defaultCharset())
            while( line = reader.readLine() )
                println line
        }

    }

    static class CmdRemove extends SubCmd {

        @Override
        int getArity() { 1 }

        @Override
        String getName() { 'rm' }

        @Override
        String getDescription() { 'Remove a file' }

        @Override
        void apply(Path source, BasicFileAttributes attrs, Path target) {
            Files.isDirectory(source) ? FilesEx.deleteDir(source) : FilesEx.delete(source)
        }

    }

    static class CmdMakeDir extends SubCmd {
        @Override
        int getArity() { 1 }

        @Override
        String getName() { 'mkdir' }

        @Override
        String getDescription() { 'Create a directory' }

        @Override
        void apply(Path source, BasicFileAttributes attrs, Path target) {
            Files.createDirectories(path)
        }

    }


    @Parameter
    List<String> args

    @Override
    String getName() {
        return NAME
    }

    @Override
    void run() {
        if( !args ) {
            usage()
            return
        }

        final base = Paths.get('.')
        final config = new ConfigBuilder()
                .setOptions(launcher.options)
                .setBaseDir(base.complete())
                .build()
        // this smells bad! the constructor below set the session as a singleton object
        new Session(config)

        final cmd = findCmd(args[0])
        if( !cmd ) {
            throw new AbortOperationException("Unknow file system command: `$cmd`")
        }

        cmd.execute(args[1..-1])
    }

    private SubCmd findCmd( String name ) {
        commands.find { it.name == name }
    }

    /**
     * Print the command usage help
     */
    void usage() {
        usage(args)
    }

    /**
     * Print the command usage help
     *
     * @param args The arguments as entered by the user
     */
    void usage(List<String> args) {

        def result = []
        if( !args ) {
            result << 'Usage: nextflow fs <command> [arg]'
            result << ''
            result << 'Commands:'
            commands.each {
            result << "  ${it.name}\t${it.description}"
            }
            result << ''
            println result.join('\n').toString()
        }
        else {
            def sub = findCmd(args[0])
            if( sub )
                println sub.usage()
            else {
                throw new AbortOperationException("Unknown cloud sub-command: ${args[0]}")
            }
        }

    }

}
