/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
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

import groovy.transform.PackageScope
import picocli.CommandLine
import picocli.CommandLine.Option
import picocli.CommandLine.Command
/**
 * Implement command shared methods
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
abstract class CmdBase implements Runnable {

    private Launcher launcher

    String getName() {
        this.class.getAnnotation(Command)?.name()
    }

    Launcher getLauncher() { launcher }

    void setLauncher( Launcher value ) { this.launcher = value }

    @Option(names=['-h','--help'], description = 'Print the command usage', arity = '0', usageHelp = true)
    boolean help

    @PackageScope
    List<? extends CmdBase> getSubCommands() { Collections.emptyList() }

    @PackageScope
    void addToCommand(CommandLine parent) {
        final name = getName()
        if( !name )
            throw new IllegalStateException("Make sure command ${this.class.simpleName} defines a name attibute using the @Command annotation")
        final children = getSubCommands()
        if( !children ) {
            parent.addSubcommand(name, this)
        }
        else {
            final cmd = new CommandLine(this)
            for( CmdBase it : children )
                it.addToCommand(cmd)
            parent.addSubcommand(name, cmd)
        }
    }

    protected void usage(String cmdName) {
        def cmd = launcher.findCommand(cmdName)
        if( cmd ) {
            launcher.usage(cmd)
        }
        else {
            println "Unknown command: $cmdName"
        }
    }

    protected void usage() {
        usage(this.getName())
    }
}