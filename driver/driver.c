#include "unp.h"

int
main() {
  int pid;
  struct sockaddr_in servaddr, cliaddr;
  
  int listenfd = socket(AF_INET, SOCK_STREAM, 0);

  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port = htons(51202);

  Bind(listenfd, (SA*)&servaddr, sizeof(servaddr));
  Signal(SIGCHLD, SIG_IGN);
  Listen(listenfd, LISTENQ);

  int infd[2];
  int outfd[2];
  pipe(infd);
  pipe(outfd);
  
  if ((pid=fork()) == 0) {
    close(infd[1]);
    close(outfd[0]);
    dup2(infd[0], 0);
    dup2(outfd[1], 1);
    execl("/usr/bin/java", "java", "-jar", "../dist/simpledb.jar", "parser", "../catalog.txt");
    exit(1);
  }

  close(infd[0]);
  close(outfd[1]);

  char starting[5000];
  while (1) {
    int m = read(outfd[0], starting, 4096); 
    if (strncmp(starting, "SimpleDB", 8) == 0) {
      break;
    }
  }
  for ( ; ; ) {
    int clilen = sizeof(cliaddr);
    int connfd = accept(listenfd, (SA*)&cliaddr, &clilen);
    int n;
    char buf[5000];
    printf("Get from a person\n");
    while ((n=read(connfd, buf, 4096)) > 0) {
      printf("statement start\n");
      write(infd[1], buf, n);
      write(1, buf, n);
      
      write(infd[1], buf, 0);
      while((n = read(outfd[0], buf, 4096)) > 0) {
        write(connfd, buf, n);
        write(1, buf, n);
        if (strstr(buf, "SimpleDB") != 0) {
          break;
        }
      }
      printf("finish one statement\n");
    }
    printf("Leave a person\n");
    close(connfd);
  }
}
