#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <chrono>

#include <unistd.h>
#include <sys/wait.h>

typedef struct{
   long int from;
     long int to;
} chunk_work;

typedef struct{
    double partial_sum;
}partial_result;

typedef struct{
    double total_sum;
}total_result;

typedef struct{
     int fd[2];
}child_chann_com;

typedef struct{
     int num_jobs;
    chunk_work* list_chunk_works;
    child_chann_com* list_child_chann_com;
     int* list_pid_slaves;
}admin_jobs;

void release_admin_jobs(admin_jobs theAdminJobs )
{
    free( theAdminJobs.list_chunk_works );
    free( theAdminJobs.list_child_chann_com );
    free( theAdminJobs.list_pid_slaves );
}

chunk_work* generate_limits_chunk_works( long int from,  long int to,  long int num_jobs )
{
     long int size_chunk = (to-from) / num_jobs;

    chunk_work* chunk_jobs = (chunk_work*) malloc( sizeof(chunk_work) * num_jobs );

    for(int ii=0; ii<num_jobs; ++ii)
    {
        chunk_jobs[ii].from =  size_chunk*ii + from;
        chunk_jobs[ii].to = size_chunk*(ii+1) + from;
    }
    chunk_jobs[num_jobs-1].to = to;

    return chunk_jobs;
}

child_chann_com* create_channel_comunication( int num_channels )
{
    child_chann_com* theChannelsCom = (child_chann_com*)malloc( sizeof(child_chann_com)*num_channels);
    for( int ii=0; ii<num_channels; ++ii)
    {
        pipe( theChannelsCom[ii].fd );
    }

    return theChannelsCom;
}

admin_jobs distribute_work( long int from,  long int to,  long int num_jobs )
{
    // populate the JCB
    admin_jobs theJobs;
    theJobs.num_jobs = num_jobs;
    theJobs.list_chunk_works = generate_limits_chunk_works( from, to , num_jobs );
    theJobs.list_child_chann_com = create_channel_comunication( num_jobs );
    theJobs.list_pid_slaves  = ( int*) malloc( (sizeof( int)*num_jobs ) );
    return theJobs;
}


long int sum_numbers_v1(  long int from_number,  long int to_number)
{
    long int sum_result = 0;

    for( long int i=from_number; i<to_number; ++i)
        sum_result += i;

    return sum_result;
}

long int sum_numbers_v2( long int from_number, long int to_number)
{
    long int sum_result = 0;
    long int i=to_number;

    do
        sum_result += i;
    while(( --i >= from_number));

    return sum_result;
}

void slave_work(  long int from,  long int to,  long int fd_pipe_out)
{
    long int partial_result = sum_numbers_v1( from, to );

    // send the partial_result to parent process
    char buffer[128];
    sprintf(buffer,"%ld",partial_result);
    write( fd_pipe_out, buffer, strlen(buffer)+1);

    // printf("Slave %d : from %d  to %d -- partial sum: %ld as str %s\n", ii, admin_slaves.list_chunk_works[ii].from,
    //                                             admin_slaves.list_chunk_works[ii].to,
    //                                             partial_result,
    //                                             buffer );
    exit(0);
}

partial_result get_slave_partial_result( long int pid_slave,  long int fd_chann_com )
{
    partial_result result;
    char buffer_recieve[128];
    int status;

    // waiting for partial results from the slaves
    waitpid( pid_slave, &status, 0);
    //unserealize the partial result 
    read( fd_chann_com, buffer_recieve, sizeof(buffer_recieve) ); 

    // dump to wrapper result 
    result.partial_sum =  atol( buffer_recieve );

    return result;
}

void assamble_total_result( total_result *theTotalResult, partial_result thePartialResult )
{
    theTotalResult->total_sum += thePartialResult.partial_sum;
}

long int parallel_sum_numbers( long int from,  long int to, long int num_sub_process )
{
    admin_jobs admin_slaves = distribute_work(from, to, num_sub_process );

    // Launch the slaves ...
    for( long int ii=0; ii<num_sub_process; ++ii)
    {
        // create slaves ...
        admin_slaves.list_pid_slaves[ii] = fork();

        if( admin_slaves.list_pid_slaves[ii] == 0 )
        {  // Child's Code
            slave_work( admin_slaves.list_chunk_works[ii].from,
                        admin_slaves.list_chunk_works[ii].to,
                        admin_slaves.list_child_chann_com[ii].fd[1] );
        }
    }

    // Wait the partial result from the slaves ...
    // compute the total result
    partial_result slave_result;
    total_result theTotalResult;
    theTotalResult.total_sum = 0;
    for(  long int ii=0; ii< num_sub_process; ++ii){
        slave_result = get_slave_partial_result( admin_slaves.list_pid_slaves[ii], 			admin_slaves.list_child_chann_com[ii].fd[0] );
        // assemble total result 
        assamble_total_result( &theTotalResult, slave_result );
    }

    release_admin_jobs( admin_slaves );

    return theTotalResult.total_sum;
}


void sum_serial()
{   
    printf(" \n\n sum numbers \n\n");

     long int from_number = 0;
     long int to_number = ( long int)1e9;

    printf( "from_number = %ld \n", from_number);
    printf( "to_number = %ld \n", to_number);

    printf("\nprocessing ...\n");

    clock_t start = clock() ;
    double result = sum_numbers_v1(from_number, to_number);
    clock_t end = clock() ;
    double elapsed_time = (end-start)/(double)CLOCKS_PER_SEC ;
    printf("\nsum from_number %ld  to_number  %ld is : %f\n", from_number, to_number, result );
    printf("\nprocess v1 took : %f secods\n", elapsed_time);


    start = clock() ;
    result = sum_numbers_v2(from_number, to_number);
    end = clock() ;
    elapsed_time = (end-start)/(double)CLOCKS_PER_SEC ;
    printf("\nsum from_number %ld  to_number  %ld is : %f\n", from_number, to_number, result );
    printf("\nprocess v2 took : %f secods\n", elapsed_time);


    printf("\n\n\n");


}

int main()
{
    //time_t tic, toc;

    printf(" \n\n sum numbers \n\n");

     long int from_number = 0;
     long int to_number = ( long int)1e10;

    printf( "from_number = %ld \n", from_number);
    printf( "to_number = %ld \n", to_number);

    printf("\nprocessing ...\n");

     int num_sub_process = 8;

    //tic = clock();

    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    long int result2 = sum_numbers_v1(from_number, to_number);

    //toc = clock();
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> time_span =   std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);


   // printf(" took %f secs\n", (double)(toc - tic)/CLOCKS_PER_SEC);
    printf(" Serial took %f secs\n", time_span.count());


   //tic = clock();
    t1 = std::chrono::high_resolution_clock::now();
    long int result = parallel_sum_numbers(from_number,to_number, num_sub_process);
    //toc = clock();
    t2 = std::chrono::high_resolution_clock::now();
    time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    //printf(" took %f secs\n", (double)(toc - tic)/CLOCKS_PER_SEC);
    printf(" took %f secs\n", time_span.count());
    printf("Parallel %d slaves--- Total result:  %ld\n\n", num_sub_process, result);


    printf("\n\n\n");

    return 0;
 }