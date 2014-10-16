#include<stdio.h>
#include<stdlib.h>
#include<math.h>
#include<string.h>
int main()
{
FILE *fp;
int b,j;
int marks,temp,s;
long long id=1;
char name[20],branch[20],sub[20];
fp=fopen("student700","w");
long long int i=0;
while(i<700000)
{
	
	for(j=0;j<10;j++)
	{
		temp=rand()%26+97;
		name[j]=(char)temp;
	}
	name[10]='\0';
	b=rand()%2;
	if(b==0)
	strcpy(branch,"cs");
	else
	strcpy(branch,"it");
	s=rand()%4;
	if(s==0)
	strcpy(sub,"cloud");
	else if(s==1)
	strcpy(sub,"softwares");
	else if(s==2)
	strcpy(sub,"programming");
	else
	strcpy(sub,"networks");			
	temp=rand()%100;
fprintf(fp,"%lld %s %s %s %d \n",id,name,branch,sub,temp);
id++;
i++;
}
return 0;
}
