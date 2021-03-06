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

package nextflow.executor
import java.nio.file.Path
import java.util.regex.Pattern

import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun
/**
 * Processor for SLURM resource manager (DRAFT)
 *
 * See http://computing.llnl.gov/linux/slurm/
 *
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class SlurmExecutor extends AbstractGridExecutor {

    static private Pattern SUBMIT_REGEX = ~/Submitted batch job (\d+)/

    /**
     * Gets the directives to submit the specified task to the cluster for execution
     *
     * @param task A {@link TaskRun} to be submitted
     * @param result The {@link List} instance to which add the job directives
     * @return A {@link List} containing all directive tokens and values.
     */
    protected List<String> getDirectives(TaskRun task, List<String> result) {

        result << '-D' << quote(task.workDir)
        result << '-J' << getJobNameFor(task)
        result << '-o' << quote(task.workDir.resolve(TaskRun.CMD_LOG))     // -o OUTFILE and no -e option => stdout and stderr merged to stdout/OUTFILE
        result << '--no-requeue' << '' // note: directive need to be returned as pairs

        if( task.config.cpus > 1 ) {
            result << '-c' << task.config.cpus.toString()
        }

        if( task.config.time ) {
            result << '-t' << task.config.getTime().format('HH:mm:ss')
        }

        if( task.config.getMemory() ) {
            result << '--mem' << task.config.getMemory().toMega().toString()
        }

        // the requested partition (a.k.a queue) name
        if( task.config.queue ) {
            result << '-p' << (task.config.queue.toString())
        }

        // -- at the end append the command script wrapped file name
        if( task.config.clusterOptions ) {
            result << task.config.clusterOptions.toString() << ''
        }

        return result
    }

    String getHeaderToken() { '#SBATCH' }

    /**
     * The command line to submit this job
     *
     * @param task The {@link TaskRun} instance to submit for execution to the cluster
     * @param scriptFile The file containing the job launcher script
     * @return A list representing the submit command line
     */
    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile ) {

        ['sbatch', scriptFile.getName()]

    }

    /**
     * Parse the string returned by the {@code sbatch} command and extract the job ID string
     *
     * @param text The string returned when submitting the job
     * @return The actual job ID string
     */
    @Override
    def parseJobId(String text) {

        for( String line : text.readLines() ) {
            def m = SUBMIT_REGEX.matcher(line)
            if( m.find() ) {
                return m.group(1).toString()
            }
        }

        // customised `sbatch` command can return only the jobid
        def id = text.trim()
        if( id.isLong() )
            return id

        throw new IllegalStateException("Invalid SLURM submit response:\n$text\n\n")
    }

    @Override
    protected List<String> getKillCommand() { ['scancel'] }

    @Override
    protected List<String> queueStatusCommand(Object queue) {

        final result = ['squeue','--noheader','-o','%i %t', '-t', 'all']

        if( queue )
            result << '-p' << queue.toString()

        final user = System.getProperty('user.name')
        if( user )
            result << '-u' << user
        else
            log.debug "Cannot retrieve current user"

        return result
    }

    /*
     *  Maps SLURM job status to nextflow status
     *  see http://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
     */
    static private Map STATUS_MAP = [
            'PD': QueueStatus.PENDING,  // (pending)
            'R': QueueStatus.RUNNING,   // (running)
            'CA': QueueStatus.ERROR,    // (cancelled)
            'CF': QueueStatus.PENDING,  // (configuring)
            'CG': QueueStatus.RUNNING,  // (completing)
            'CD': QueueStatus.DONE,     // (completed)
            'F': QueueStatus.ERROR,     // (failed),
            'TO': QueueStatus.ERROR,    // (timeout),
            'NF': QueueStatus.ERROR,    // (node failure)
            'S': QueueStatus.HOLD,      // (job suspended)
            'ST': QueueStatus.HOLD,     // (stopped)
            'PR': QueueStatus.ERROR,    // (Job terminated due to preemption)
            'BF': QueueStatus.ERROR,    // (boot fail, Job terminated due to launch failure)
    ]

    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {

        def result = [:]

        text.eachLine { String line ->
            def cols = line.split(/\s+/)
            if( cols.size() == 2 ) {
                result.put( cols[0], STATUS_MAP.get(cols[1]) )
            }
            else {
                log.debug "[SLURM] invalid status line: `$line`"
            }
        }

        return result
    }
}
