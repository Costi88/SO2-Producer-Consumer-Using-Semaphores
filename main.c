#include <stdio.h> //Din stdio.h am folosit: printf, perror.
#include <stdlib.h> //Din stdlib.h am folosit: exit, srand.
#include <pthread.h> //Din pthread.h am folosit: pthread_t, pthread_create, pthread_join.
#include <semaphore.h> //Din semaphore.h am folosit: sem_t, sem_init, sem_wait, sem_post, sem_destroy.
#include <unistd.h> //Din unistd.h am folosit: sleep.
#include <time.h> //Din time.h am folosit: time.

//Dimensiunea bufferului.
#define SIZE 10
//Numarul de produse create.
#define ITEMS 20
//Numarul de threaduri consumer.
#define NR_CONSUMERS 4
//Numarul de threaduri producer.
#define NR_PRODUCERS 2
//Numarul total de threaduri.
#define NR_THREADS (NR_PRODUCERS + NR_CONSUMERS)

//Bufferul din memoria partajata.
int buffer[SIZE];
//Contor pentru produsele create.
int produced = 0;
//Contor pentru produsele consumate.
int consumed = 0;

sem_t empty;
sem_t full;
sem_t sem;

void* producer(void* arg) {
    while (1) {
        //Asteptam sa se elibereze locuri.
        sem_wait(&empty);
        //Intram in sectiunea critica.
        sem_wait(&sem);

        //Daca toate produsele au fost create iesim din while.
        if(produced >= ITEMS) {
            //Iesim din sectiunea critica.
            sem_post(&sem);
            //Incrementam valoarea semaforului si deblocam procesele 
            //care asteptau daca este cazul.
            sem_post(&full);
            break;
        }
        
        //Calculam indexul curent.
        int index = produced % SIZE;
        //Cream un produs nou.
        buffer[produced % SIZE] = rand() % 100;
        printf("Producer %lu | Buffer index: %d | Item: %d\n", (unsigned long) pthread_self(), index, buffer[produced % SIZE]);
        
        //Incrementam numarul de produse create.
        produced++;

        //Iesim din sectiunea critica.
        sem_post(&sem);
        //Incrementam valoarea semaforului si deblocam procesele 
        //care asteptau daca este cazul.
        sem_post(&full);
    }

    return NULL;
}

void* consumer(void* arg) {
    while (1) {
        //Asteptam sa se adauge produse.
        sem_wait(&full);
        //Intram in zona critica.
        sem_wait(&sem);

        //Daca toate produsele au fost consumate iesim din while.
        if (consumed >= ITEMS) {
            //Iesim din sectiunea critica.
            sem_post(&sem);
            //Incrementam valoarea semaforului si deblocam procesele 
            //care asteptau daca este cazul.
            sem_post(&full);
            break;
        }

        //Calculam indexul curent.
        int index = consumed % SIZE;
        //Afisam produsul consumam.
        printf("Consumer %lu | Buffer index: %d | Item: %d\n", (unsigned long) pthread_self(), index, buffer[index]);

        //Incrementam numarul de produse consumate.
        consumed++;

        //Iesim din sectiunea critica.
        sem_post(&sem);
        //Incrementam valoarea semaforului si deblocam procesele 
        //care asteptau daca este cazul.
        sem_post(&empty);
    }

    return NULL;
}

/**
 * Functia va afisa un mesaj de eroare si va opri executia programului.
 * @param msg Mesajul de eroare care va fi afisat.
 */
void stop_exec(char msg[]) {
    perror(msg);
    exit(1);
}

/**
 * Creaza NR_PRODUCERS threaduri de tip producer si NR_CONSUMERS threaduri de tip consumer.
 * Daca un thread nu o sa poata fi creat se va afisa un mesaj de eroare si executia se va opri.
 * @param threads Threadurile care vor fi create.
 */
void create_threads(pthread_t threads[]) {
    for (int i = 0; i < NR_PRODUCERS; i++)
        if (pthread_create(&threads[i], NULL, producer, NULL) != 0)
            stop_exec("Creating producer thread failed");

    for (int i = NR_PRODUCERS; i < NR_THREADS; i++)
        if (pthread_create(&threads[i], NULL, consumer, NULL) != 0)
            stop_exec("Creating consumer thread failed");
}

/**
 * Sincronizeaza threadurile astfel incat programul sa nu-si continue 
 * executia pana cand threadurile isi termina executia.
 * Daca nu pot fi sincronizate toate threadurile atunci se 
 * va afisa un mesaj de eroare si executia se va opri.
 * @param threads Threadurile care vor fi sincronizate.
 */
void sync_threads(pthread_t threads[]) {
    for (int i = 0; i < NR_THREADS; i++)
        if (pthread_join(threads[i], NULL) != 0)
            stop_exec("Join failed");
}

int main() {
    srand(time(NULL));

    pthread_t threads[NR_THREADS];

    //Initializarea unui semafor binar.
    sem_init(&sem, 0, 1);
    //Initializarea unui semafor care urmareste locurile libere din buffer.
    sem_init(&empty, 0, SIZE);
    //Initializarea unui semafor care urmareste elementele de consumat din buffer.
    sem_init(&full, 0, 0);

    create_threads(threads);

    sync_threads(threads);

    //Distrugem semafoarele
    sem_destroy(&empty);
    sem_destroy(&full);
    sem_destroy(&sem);

    return 0;
}
