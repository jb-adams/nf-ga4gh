/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.ga4gh.tes.executor

import nextflow.ga4gh.tes.client.ApiException
import nextflow.ga4gh.tes.client.api.TaskServiceApi
import nextflow.ga4gh.tes.client.model.TesCreateTaskResponse
import nextflow.ga4gh.tes.client.model.TesTask
import nextflow.processor.TaskStatus
import nextflow.util.Duration

import java.nio.file.Path
import java.nio.file.Paths

import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import spock.lang.Specification
/**
 * @author Emilio Palumbo <emilio.palumbo@crg.eu>
 */
class TesTaskHandlerTest extends Specification {

    def 'should normalize canonical and legacy az urls to storage-account az format'() {

        given:
        def executor = Mock(TesExecutor)
        def task = Mock(TaskRun) {
            getWorkDir() >> Paths.get('.')
        }
        def handler = new TesTaskHandler(task, executor)
        def method = TesTaskHandler.getDeclaredMethod('fixTesAzureLocalPath', String)
        method.accessible = true

        when:
        def canonical = method.invoke(handler, 'az://mystorage/inputs/work/file.txt')
        def legacy = method.invoke(handler, 'az://inputs/work/file.txt')

        then:
        canonical == 'az://mystorage/inputs/work/file.txt'
        legacy == 'az://mystorage/inputs/work/file.txt'
        2 * executor.getAzureStorageAccount() >> 'mystorage'
    }

    def 'should set resources' () {

        given:
        def executor = Mock(TesExecutor)
        def task = Mock(TaskRun) {
            getInputFilesMap() >> [:]
            getOutputFilesNames() >> []
            getOutputsByType(_) >> [:]
        }
        task.getName() >> 'tes-task'
        task.getWorkDir() >> Paths.get(".")
        task.getConfig() >> new TaskConfig(memory: '2GB', cpus: 4, disk: '10GB')
        task.getContainer() >> 'foo'
        def handler = new TesTaskHandler(task, executor)


        when:
        def t = handler.newTesTask()

        then:
        t.getResources().cpuCores == 4
        t.getResources().ramGb == 2
        t.getResources().diskGb == 10
        t.getResources().preemptible == null
        t.getResources().zones == null

    }

    def 'should apply tags to TES task'() {
        given:
        def executor = Mock(TesExecutor)
        def task = Mock(TaskRun) {
            getInputFilesMap() >> [:]
            getOutputFilesNames() >> []
            getOutputsByType(_) >> [:]
        }
        task.getName() >> 'tes-task'
        task.getWorkDir() >> Paths.get('.')
        task.getConfig() >> new TaskConfig()
        task.getContainer() >> 'foo'
        def handler = new TesTaskHandler(task, executor)

        when:
        def t = handler.newTesTask()

        then:
        1 * executor.getTags() >> [env: 'prod', team: 'bio']
        t.getTags() == [env: 'prod', team: 'bio']
    }

    def 'should submit job' () {

        given:
        def executor = Mock(TesExecutor)
        def task = Mock(TaskRun)
        def bashBuilder = Mock(TesBashBuilder)
        def client = Mock(TaskServiceApi)
        def handler = Spy(TesTaskHandler)
        handler.@client = client
        handler.@executor = executor
        handler.task = task

        def req = Mock(TesTask)
        def resp = Mock(TesCreateTaskResponse)

        when:
        handler.submit()
        then:
        1 * executor.getRemoteBinDir() >> Path.of("/work/bin")
        1 * handler.newTesBashBuilder(task, "/work/bin") >> bashBuilder
        1 * bashBuilder.build() >> null
        1 * handler.newTesTask() >> req
        1 * client.createTask(req) >> resp
        1 * resp.getId() >> '12345'

        handler.status == TaskStatus.SUBMITTED
        handler.requestId == '12345'
    }

    def 'should tell transient errors from permanent ones' () {
        expect:
        TesTaskHandler.isTransientError(ERROR) == EXPECTED

        where:
        ERROR                                                   | EXPECTED
        new ApiException(new ConnectException('refused'))       | true      // no response: code 0
        new ApiException(503, 'unavailable')                    | true
        new ApiException(500, 'error')                          | true
        new IOException('reset')                                | true
        new RuntimeException(new ConnectException('refused'))   | true
        new ApiException(404, 'not found')                      | false
        new ApiException(401, 'unauthorized')                   | false
        new IllegalStateException('bug')                        | false
    }

    def 'should retry transient status errors until the poll error timeout' () {

        given:
        def executor = Mock(TesExecutor) { getPollErrorTimeout() >> Duration.of('10m') }
        def client = Mock(TaskServiceApi)
        def handler = Spy(TesTaskHandler)
        handler.@client = client
        handler.@executor = executor
        handler.@requestId = '12345'
        handler.task = Mock(TaskRun) { getName() >> 'foo (1)' }
        def task = new TesTask()

        when: 'the TES server is unreachable'
        def first = handler.getTaskStatus()
        then: 'the poll is treated as "no change yet"'
        1 * client.getTask('12345', null) >> { throw new ApiException(new ConnectException('refused')) }
        first == null
        handler.@pollErrorsSinceMillis != null

        when: 'the TES server answers again'
        def second = handler.getTaskStatus()
        then: 'the status is returned and the error timer is reset'
        1 * client.getTask('12345', null) >> task
        second.is(task)
        handler.@pollErrorsSinceMillis == null

        when: 'requests keep failing, still within the timeout'
        handler.@pollErrorsSinceMillis = System.currentTimeMillis() - Duration.of('9m').toMillis()
        def third = handler.getTaskStatus()
        then:
        1 * client.getTask('12345', null) >> { throw new ApiException(503, 'unavailable') }
        third == null

        when: 'errors last longer than the timeout'
        handler.@pollErrorsSinceMillis = System.currentTimeMillis() - Duration.of('11m').toMillis()
        handler.getTaskStatus()
        then: 'the error is raised, failing the task'
        1 * client.getTask('12345', null) >> { throw new ApiException(503, 'unavailable') }
        thrown(ApiException)
    }

    def 'should not retry permanent status errors' () {

        given:
        def client = Mock(TaskServiceApi)
        def handler = Spy(TesTaskHandler)
        handler.@client = client
        handler.@executor = Mock(TesExecutor)
        handler.@requestId = '12345'

        when:
        handler.getTaskStatus()
        then:
        1 * client.getTask('12345', null) >> { throw new ApiException(404, 'not found') }
        thrown(ApiException)
    }

}
