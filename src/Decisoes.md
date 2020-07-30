# Decisões

## Estrutura dos Dados

Foi escolhido o ```ArrayList``` para armazenar os eventos em memória.

Os eventos são armazenados ordenados pelo ```timestamp``` priorizando-se a _performance_ de leitura. Isso gera uma 
penalização na _performance_ de escrita. Uso-se o algoritmo de _Binary Insertion Sort_ para inserção, com custo de 
O(log n) para achar a posição e O(n) para a inserção. Embora o custo de _swap_ dos elementos do array faça o custo de pior caso se manter em O(n^2).

Pode-se ajustar a capacidade inicial para minimizar as realocações do array.

Com o crescimento dos dados essa opção degrada em performance.

## Comportamento

Optou-se por tentar, o tanto quanto possível, manter atualizados todos os iteradores enquanto chegarem eventos dentro da 
faixa de início e fim especificada, contemplando-se os casos de:
* remoções realizadas por outros iteradores
* chegada de eventos fora de ordem
* chegada de novos eventos após abertura do iterador

Considerou-se que os eventos possam chegar fora de ordem e que mesmo eventos com _timestamp_ posterior em relação a 
posição corrente do iterador devem ser refletidos ao avançar o iterador. 

Ainda assim, alguns casos ficaram de fora:
* chegada de evento anterior a posição atual do iterador
* alguns casos de testes de fim do array (por falta de tempo para implementar)
* remoção de todos os eventos de um determinado tipo

O teste desse comportamento for implementado replicando-se várias situações.

## Estratégias de _lock_

Foram experimentadas 3 opções de _lock_ para garantir o uso concorrente do ```EventStore```:
* _synchronized_
* _read write_ com opção de _fair_ para tentar minimizar _starvation_
* optimistic read

Os experimentos estão no pacote `net.intelie.challenges.exploration`.

Optou-se pelo _synchronized_ para a solução final, por ser uma estratégia que costuma dar resultados consistentes em situações diversas.

A estratégia _read_write_ seria interessante, pois o algoritmo de checagem da posição do iterador para se acomodar a 
novos dados precisa de muitas leituras. Mas na presença de muitos _writers_, isso pode não ser relevante.
Foi iniciado um código para avaliação das _performances_, mas o código não foi finalizado a tempo da entrega desse 
trabalho.

### Evidência de _Thread Safety_

Foram implementados dois testes para exercitar a concorrência no acesso ao ```EventStore```.
* `ThreadSafetyTest.testInsertThreadSafety()`
* `ThreadSafetyTest.givenProducersAndConsumers_thenTestRemoveThreadSafety()`

Embora estes testes não sejam determinísticos, foi possível verificar falhas ao usar o ```EventStoreUnsafe```.
E todos os ```EventStore``` protegidos, rodaram com sucesso nesses testes.

Claro, que a melhor garantia nesse caso ainda é a inspeção do código.

Também a implementação do teste para o comportamento de atualização dos ```consumers``` na presença de remoções e 
inserções é uma forte evidência da proteção a concorrência. Este teste foi implementado de forma determinística 
lançando-se mão do ```CountDownLatch``` para geração das escritas e leituras replicando as situações sensíveis.

### Benchmarks

Foram experimentadas as 3 estratégias de _lock_ para tentar verificar vantagens e desvantagens em dois cenários:
* mesmo número de _readers_ e _writers_
* muitos _readers_ e poucos _writers_

Os resultados seguem abaixo:

    Benchmark                              Mode  Cnt    Score     Error  Units
    Benchmarks.optimistic                 thrpt    5  407.413 ±  52.325  ops/s
    Benchmarks.optimmisticWithFewWriters  thrpt    5  679.074 ±  67.019  ops/s
    Benchmarks.rw                         thrpt    5   28.938 ±  19.036  ops/s
    Benchmarks.rwWithFewWriters           thrpt    5  458.657 ± 160.841  ops/s
    Benchmarks.synch                      thrpt    5  367.405 ±  55.519  ops/s
    Benchmarks.synchWithFewWriters        thrpt    5  651.351 ±  70.869  ops/s

O _optimistic lock_ e o _synchronized_ obtiveram resultados similares.
 
O _optimistic lock_ não foi 100% implementado, com um trecho da busca da posição correta no avanço do iterador usando 
_read lock_. Optou-se por descartar os seus resultados de _benchmark_.

Os resultados do _read write lock_ também parecem inconsistentes, sugerindo que o código de _benchmark_ ainda não está 
satisfatório.