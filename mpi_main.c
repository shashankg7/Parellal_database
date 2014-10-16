#include<stdio.h>
#include<string.h>
#include<regex.h>
#include<stdlib.h>
#include<mpi.h>
#include<time.h>
struct record
{	
	char field[20];
	char operator;
	int value;           // to hold int values in where condition
	char string_value[20];   // to hold string value
}condition[2];
struct Table_Rec
{
       long long int prim_id;
       char col2[20];
       char col3[20];
       char col4[20];
       int col5;
};
int no_condition=0;
int col_ind[5]={0};
int match(char text[], char pattern[]) ;
void ExtractCondition_Clause(char *query);
int select_retrieve();
static int compile_regex (regex_t * r, const char * regex_text);
void create_metadataFile(char File1 [],char tableName[],int no_proc);
int join_function(char table1[] , char table2[]);
int Data_Processor(char tablename[]);
/***********************   
1. max 5 col can be selected in query 
2. It compares only lower case 'select'
3. Col names can have arbitary spaces and  comma between them
4.put single space between table name and from and where
**********************************/
int main(int argc,char *argv[]) {
clock_t begin,end;
double time;
    char *ptr;
	begin=clock();
	char split_query[50],display_part[50],temp[20],q[50],col_names[5][20];
    int position,index=0,i=0,j=0;
    char *query = argv[1];
     



    while((int)*(query+i)!=0)
    {
	 q[i]=(*(query+i));
	 i++;
    }
q[i]='\0';
*(query+i)='\0';
printf("value of i : %d  %c\n",i, *(query+i));
	i=0;
	while(i<5)
	 {
 		memset(col_names[i], '\0', sizeof(col_names[i]));    // col_names store name of coloumns to be printed in query
		 i++;
	 }
	i=0;

    char *c = strtok(q," ");      // to extract first word from query
	int pos_for_table;
	
	if(strcmp(c,"select")==0)
	{
           query = &q[7];

		memset(split_query, '\0', sizeof(split_query));
		strcpy(split_query,query);  		// part of query after 'select'  removing 'select'

		position = match(split_query, "from");		//finding last index before 'f' in from 
	        pos_for_table = position;
		memset(display_part, '\0', sizeof(display_part));
		memcpy(display_part,split_query,position);  //part of the query having coloumns names to printed 
		
          char *pch = strtok (display_part,", ");
          while (pch != NULL)   // loop to extract coloum name
          {
            memset(col_names[i], '\0', sizeof(col_names[i]));
            strcpy(col_names[i],pch);
   
            printf ("mmm%smhmhm %d\n",col_names[i],i);
            pch = strtok (NULL, ", ");
            i++;
         }
      position = match(split_query,"where");
      query = query+position+6;
       ExtractCondition_Clause(query);   //to extract where condition

		char *r = &split_query[pos_for_table+5];
	
		memset(display_part, '\0', sizeof(display_part));
		memcpy(display_part,r,position-pos_for_table-6);


	printf("Table name \n%s\n",display_part);

		FILE *f;
		int col_indx;
		char col_name[20];


	if(strcmp(display_part,"student")==0)
	{
		f = fopen("hash_student","r");
		if(strcmp(col_names[0],"*")==0)
		{
			for(j=0;j<5;j++){
				col_ind[j]=1;
			}
printf("\n* if called\n")		;
		}
    		else{
			while(fscanf(f,"%d %s",&col_indx,col_name)!=EOF)
			{
				for(j=0;j<i;j++)
				{
				
					if(strcmp(col_name,col_names[j])==0)
					{	
						col_ind[col_indx] = 1;				
						printf("\ncolName %s %d\n",col_name,col_indx);
					} 
	
				
				}
			}
		}
	}
	else if(strcmp(display_part,"personal_rec")==0)
	{
		f = fopen("hash_person_rec","r");

		if(strcmp(col_names[0],"*")==0)
		{
			for(j=0;j<5;j++){
				col_ind[j]=1;
			}		
		}
		else
		{
			while(	fscanf(f,"%d %s",&col_indx,col_name)!=EOF)
			{
				for(j=0;j<i;j++)
				{
					if(strcmp(col_name,col_names[j])==0)
					{
						col_ind[col_indx] = 1;				
					}
	
				}
			}			
		}
}
else if(strcmp(display_part,"department")==0)
	{
		f = fopen("hash_department","r");

		if(strcmp(col_names[0],"*")==0)
		{
			for(j=0;j<i;j++){
				col_ind[j]=1;
			}		
		}
		else
		{
			while(fscanf(f,"%d %s",&col_indx,col_name)!=EOF)
			{
				for(j=0;j<i;j++)
				{
					if(strcmp(col_name,col_names[j])==0)
					{
						col_ind[col_indx] = 1;				
					}
	
				}
			}			
		}


	}    

Data_Processor(display_part);   //passing table name and no of process(nodes)
}
else{
	char table_names[2][20],colm_name[20],filename1[20]="hash_",filename2[10]="hash_",c_name[10];
	int k=0,col_indx1,col_inx2;
	FILE *f1,*f2;


	  	query = &q[5];
		memset(split_query, '\0', sizeof(split_query));
		strcpy(split_query,query);
	 	position = match(split_query, "on");		//finding last index before 'f' in from 
	        
		memset(display_part, '\0', sizeof(display_part));
		memcpy(display_part,split_query,position); 
		
		char *pch = strtok (display_part,", ");
	          while (pch != NULL)   // loop to extract table name
	          {
	            memset(table_names[k], '\0', sizeof(table_names[k]));
	            strcpy(table_names[k],pch);
	   
	            printf ("%s %d\n",table_names[k],k);
	            pch = strtok (NULL, ", ");
	            k++;
	         }

		
 join_function(table_names[0],table_names[1]);
		
} 
	end=clock();
	time=(double)(end-begin)/CLOCKS_PER_SEC;
	printf("time spend is %lf",time);
	return 0;
}

int match(char text[], char pattern[]) {
  int c, d, e, text_length, pattern_length, position = -1;
 
  text_length    = strlen(text);
  pattern_length = strlen(pattern);
 
  if (pattern_length > text_length) {
    return -1;
  }
 
  for (c = 0; c <= text_length - pattern_length; c++) {
    position = e = c;
 
    for (d = 0; d < pattern_length; d++) {
      if (pattern[d] == text[e]) {
        e++;
      }
      else {
        break;
      }
    }
    if (d == pattern_length) {
      return position;
    }
  }
 
  return -1;
}

static int compile_regex (regex_t * r, const char * regex_text)
{
    int status = regcomp (r, regex_text, REG_EXTENDED|REG_NEWLINE);
    return 0;
}

/*
  Match the string in "to_match" against the compiled regular
  expression in "r".
 */

static int match_regex (regex_t * r, const char * to_match)
{
    /* "P" is a pointer into the string which points to the end of the
       previous match. */
    const char * p = to_match;
    /* "N_matches" is the maximum number of matches allowed. */
    const int n_matches = 15;
    /* "M" contains the matches found. */
    regmatch_t m[n_matches];

   
        int i = 0,j=0,t=0,z=0;
        char temp[10];
        int nomatch = regexec (r, p, n_matches, m, 0);
        if (nomatch) {
           
            return nomatch;
        }
         for (i = 1; i < n_matches; i++) {
           int finish=m[i].rm_eo-1;
            int start=m[i].rm_so;
          
             if (m[i].rm_so == -1) {
                break;
            }
			t=0;
		
            	if(i==1)
            	{
            			for(j=start;j<=finish;j++)
            			{
            	  			condition[no_condition].field[t++]=*(to_match+j);
            			}
            			
            	}
            	else if(i==2)
            	{
            		
            		
            		for(j=start;j<=finish;j++)
            		{
            	  		condition[no_condition].operator=*(to_match+j);
            	  	}
            	  	
            	}	
            	 else if(i==3) 
            	 {
            	 	j= start;
printf("\n\ninside\n\n");
            	 	if(*(to_match+j)>='0' && *(to_match+j)<='9')
            	 	{
            	 		
            	 		  for(j=start;j<=finish;j++)
            				{		
            	 				temp[t++]=*(to_match+j); 
            				}
            				 
            				 condition[no_condition].value=atoi(temp);
            				 
            	 	}else
            	 	{
            	 		for(j=start;j<=finish;j++)
            			{
					if(*(to_match+j)!="")	
	            	  		condition[no_condition].string_value[t++]=*(to_match+j);

    	        		} // loop for
    	        		
            	 	printf("\n%s\n",condition[no_condition].string_value);
            	 	} //END else
            	 		
                }//if i==3
            	 	
           } //for loop
       
            	 	no_condition++;
   
    return 0;
}

void ExtractCondition_Clause(char *query)
{
    regex_t r;
    const char * regex_text;
    const char * find_text;
    
        regex_text = "([a-z|A-Z|]+)([=|<|>|&|!])([a-z|A-Z|0-9]+)";
        find_text = query;
    
    
    compile_regex(& r, regex_text);
    match_regex(& r, find_text);
    regfree (& r);
   
}

/***************************************** CODE FOR SELECT QUERY ********************************************/
int Data_Processor(char tablename[])
{

	
       
	int rank,no_proc;
	char *query;

	//query=argv[0];
	//r=query_parser(query);
        int f=MPI_Init(NULL,NULL);
        MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	 MPI_Comm_size(MPI_COMM_WORLD, &no_proc);
        char c[30];
	memset(c,'\0',sizeof(c));
        strcpy(c,tablename);
	strcat(c,"meta");   	//name of metadata file  in c ex: "studentmeta" 

	create_metadataFile(c,tablename,no_proc);

/********************* code for rank 0 proc ***************************************/
        if(rank==0) 
	{
		printf("\nin rank %d\n",rank);
		int marks;
		long int no_rec; // no_rec to be accessed by each process
        	char name[10],subject[10],branch[10]; 
        	long long int offset=0,proc_id,i=0,id;

              	FILE *fh1,*fh2; 
		fh2 = fopen(tablename,"r");	//main table
	        fh1=fopen(c,"r");      //metadata file
		int flag,flag_newline;
          	fscanf(fh1,"%lld %lld %ld",&proc_id,&offset,&no_rec);
	
		printf("%s %c %s",condition[0].field,condition[0].operator,condition[0].string_value);
printf("\n\n col indx %d",col_ind[0]);
		while(proc_id!=rank)
		{
			fscanf(fh1,"%lld %lld %ld",&proc_id,&offset,&no_rec);

	
		}


		//AT THIS POINT OFFSET HAS VALUE OF OFFSET CORRESPONDING TO RANK OF THE PROCESS
		fseek(fh2,offset,SEEK_SET);
		//printf("This data is from rank 0\n");
		while(fscanf(fh2,"%lld %s %s %s %d",&id,name,branch,subject,&marks)!=EOF && i<no_rec)
		{
		flag=0;
		flag_newline=0;
		
		//printf("from rank0 %d\n",id);

		if(col_ind[0]==1 )
		{	
			if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value)
						{
						printf("%d ",id);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value)
						{
						printf("%d ",id);
						flag_newline=1;
						}
						
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value)
						{
						printf("%d ",id);
						flag_newline=1;
						}
				}
				
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0)
						{
						printf("%d ",id);
						flag_newline=1;
						}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0)
						{
						printf("%d ",id);
						flag_newline=1;
						}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%d ",id);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<<condition[0].value){
						printf("%d ",id);
						flag_newline=1;}
						
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%d ",id);
						flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(marks>>condition[0].value){
						printf("%d ",id);
						flag_newline=1;
						}
						
				}
					
			}
			
		}
		if(col_ind[1]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%s ",name);
						flag_newline=1;}
					
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value){
						printf("%s ",name);
						flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%s ",name);
						flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<<condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("requird result %s ",name);
										flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(marks>>condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			
		}
		if(col_ind[2]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<<condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(id>>condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<<condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(marks>>condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			
		}
		if(col_ind[3]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(id>>condition[0].value){
						printf("%s ",subject);
						}
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(marks>condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			
		}
                if(col_ind[4]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(marks>condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}


			}
			
		}
		if(flag_newline==1)
		printf("\n");		
		i++;

		}
		fclose(fh2);
        }

//END of rank 0 code/
/************************************ else part **************************************************/
        else
        {
		int marks;
		long int no_rec; 
        	char name[10],subject[10],branch[10]; 
                FILE *fh1,*fh2;
	        char buf1[100];
		long long int offset,proc_id,i=0,id;

		fh2 = fopen("student","r");
                fh1 = fopen("metadata","r");
                fscanf(fh1,"%lld %lld %ld",&proc_id,&offset,&no_rec);
		int flag_newline=0;
		while(proc_id!=rank)
		{
			fscanf(fh1,"%lld %lld %ld",&proc_id,&offset,&no_rec);
		}
		fseek(fh2,offset,SEEK_SET);
		//printf("This data is from rank 1\n");
		
		while(i<no_rec && fscanf(fh2,"%lld %s %s %s %d",&id,name,branch,subject,&marks)!=EOF)
		{
		flag_newline=0;
                if(col_ind[0]==1)
		{	
			if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%d ",id);
										flag_newline=1;}
						
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%d ",id);
										flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value){
						printf("%d ",id);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%d ",id);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%d ",id);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%d ",id);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%d ",id);
										flag_newline=1;}
						
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%d ",id);
										flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(marks>>condition[0].value){
						printf("%d ",id);
										flag_newline=1;}
						
				}
					
			}
			
		}
		if(col_ind[1]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(id>>condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0)
						{
						printf("%s ",name);
										flag_newline=1;}
						
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%s ",name);
										flag_newline=1;}
						
				}
				if(condition[0].operator=='>')
				{
					if(marks>condition[0].value){
						printf("%s ",name);
						flag_newline=1;}
				}
					
			}
			
		}
		if(col_ind[2]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(marks>condition[0].value){
						printf("%s ",branch);
						flag_newline=1;}
				}
					
			}
			
		}
		if(col_ind[3]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(marks>condition[0].value){
						printf("%s ",subject);
						flag_newline=1;}
				}
					
			}
			
		}
                if(col_ind[4]==1)
		{
		if(strcmp(condition[0].field,"id")==0)
			{
				if(condition[0].operator=='<')
					if(id<condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(id==condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(id>condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"name")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(name,condition[0].string_value)==0){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"branch")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(branch,condition[0].string_value)==0){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"subject")==0)
			{
				if(condition[0].operator=='&')
				{
					if(strcmp(subject,condition[0].string_value)==0){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
			if(strcmp(condition[0].field,"marks")==0)
			{
				if(condition[0].operator=='<')
					if(marks<condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				if(condition[0].operator=='=')
				{
					if(marks==condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
				if(condition[0].operator=='>')
				{
					if(marks>condition[0].value){
						printf("%d ",marks);
						flag_newline=1;}
				}
					
			}
		
		}
		if(flag_newline==1)
		printf("\n");
		i++;
		}
		fclose(fh2);
	}
MPI_Finalize();
return 0;
}	
/***************************************** JOIN FUNCTION   *****************88********************************/

int join_function(char table1[] , char table2[])
{
    
	int rank,procs;
	long int no_rec;
	FILE *f1,*f2;
	int f=MPI_Init(NULL,NULL);
        MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	 MPI_Comm_size(MPI_COMM_WORLD, &procs);
            	f1 = fopen(table1,"r");
        f2 = fopen(table2,"r");

        int flag=0;

	
	 fseek(f1,0,SEEK_END);
	long long int off1 = ftell(f1);
	 fseek(f2,0,SEEK_END);
	long long int off2 =ftell(f2);


	if(off1>off2)
	{
	
          create_metadataFile("metadataFile1",table1,procs);
          flag =1;
         }    
	else{

		  create_metadataFile("metadataFile2",table2,procs);
		
		}	

		
	if(rank==0)
	{
		struct Table_Rec table_rec[2];
                
	     FILE *stud,*mainFile1,*metaFile,*mainFile2;
	     int proc_id,loop=0;
	     long int no_rec;   
	    
	     long long int offset;
		mainFile1=fopen(table1,"r");
		    mainFile2= fopen(table2,"r");

		if(flag)
		        metaFile= fopen("metadataFile1","r");
        	else
			metaFile = fopen("metadataFile2","r");
				
		do
		{
			fscanf(metaFile,"%d %lld %ld\n",&proc_id,&offset,&no_rec);
		
		}while(proc_id!=rank);

		if(flag)
		{	
			printf("inside flag   no rec %d",no_rec);
			fseek(mainFile1,offset,SEEK_SET);
          		  while(fscanf(mainFile1,"%lld %s %s %s %d",&table_rec[0].prim_id,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,&table_rec[0].col5)!=EOF && loop < no_rec)
              		  {	  fseek(mainFile2,0,SEEK_SET);
             	             while(fscanf(mainFile2,"%lld %s %s %s %d",&table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,&table_rec[1].col5)!=EOF)
             	              {
			    	 if(table_rec[0].prim_id == table_rec[1].prim_id)
                            	 {
                             	    printf("%lld %s %s %s %d %s %s %s %d\n",table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,table_rec[1].col5,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,table_rec[0].col5);      
			              
                                 }
                                                     
                              }

				loop++;
                            }
                 }	
		else
		{
			printf("inside else");
            fseek(mainFile2,offset,SEEK_SET);	
            while(fscanf(mainFile2,"%lld %s %s %s %d",&table_rec[0].prim_id,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,&table_rec[0].col5)!=EOF && loop < no_rec)
            {
			              		  	fseek(mainFile1,0,SEEK_SET);
                       while(fscanf(mainFile1,"%lld %s %s %s %d",&table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,&table_rec[1].col5)!=EOF)
                       {
                             if(table_rec[0].prim_id == table_rec[1].prim_id)
                             {
                             	    printf("%lld %s %s %s %d %s %s %s %d\n",table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,table_rec[1].col5,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,table_rec[0].col5);      
			   
                             }                          
                       }
                 loop++;                                                     
            }
            
          }
	        
	}
	else
	{
printf("in rank 2");
	    struct Table_Rec table_rec[2];
               
	     FILE *stud,*mainFile1,*metaFile,*mainFile2;
	     int proc_id,loop=0;
	     long int no_rec;   
	    
	     long long int offset;
		mainFile1=fopen(table1,"r");
	    mainFile2= fopen(table2,"r");
		if(flag)
        metaFile= fopen("metadataFile1","r");
        else
		metaFile = fopen("metadataFile2","r");
				
		do
		{
			fscanf(metaFile,"%d %lld %ld\n",&proc_id,&offset,&no_rec);
		}while(proc_id!=rank);
		if(flag)
		{
    		fseek(mainFile1,offset,SEEK_SET);

            while(fscanf(mainFile1,"%lld %s %s %s %d",&table_rec[0].prim_id,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,&table_rec[0].col5)!=EOF && loop < no_rec)
            {
	                   fseek(mainFile1,0,SEEK_SET);
                       while(fscanf(mainFile2,"%lld %s %s %s %d",&table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,&table_rec[1].col5)!=EOF)
                       {
                             if(table_rec[0].prim_id == table_rec[1].prim_id)
                             {
                             	    printf("%lld %s %s %s %d %s %s %s %d\n",table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,table_rec[1].col5,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,table_rec[0].col5);      
			   
                             }                          
                       }
                    loop++;                                                  
            }
         }	
		else
		{
            fseek(mainFile2,offset,SEEK_SET);	
            while(fscanf(mainFile2,"%lld %s %s %s %d",&table_rec[0].prim_id,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,&table_rec[0].col5)!=EOF && loop < no_rec)
            {              		  	fseek(mainFile1,0,SEEK_SET);
                       while(fscanf(mainFile1,"%lld %s %s %s %d",&table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,&table_rec[1].col5)!=EOF)
                       {
                             if(table_rec[0].prim_id == table_rec[1].prim_id)
                             {
                             	    printf("%lld %s %s %s %d %s %s %s %d\n",table_rec[1].prim_id,table_rec[1].col2,table_rec[1].col3,table_rec[1].col4,table_rec[1].col5,table_rec[0].col2,table_rec[0].col3,table_rec[0].col4,table_rec[0].col5);      
			   
                             }                          
                       }
                   loop++;                                                   
            }
            
          }
	        
	}
MPI_Finalize();

} 
void create_metadataFile(char File1[],char tableName[],int no_proc)
{
	

	int marks,metadata_rank=0,last_id,loop=0;
	long long int id=0,j=0;
	long int  no_rec;
	char name[20],branch[20],subject[20];
	FILE *stud,*meta;

	meta=fopen(File1,"w");
	stud=fopen(tableName,"r");
	long long int offset=-1;

	while(fscanf(stud,"%lld %s %s %s %d",&id,name,branch,subject,&marks)!=EOF){loop++;}


	fseek(stud,0,SEEK_SET);


		id=0;
	no_rec = loop/no_proc;

	do{
             // add offset for only those ids which are multiple of last_id/no_process

		if(j%(no_rec)==0)
		{
			
 		  offset = ftell(stud);
			if(j==0)
      			{
				 fprintf(meta,"%d 0 %ld\n",metadata_rank++,no_rec);



			}else{
		  fprintf(meta,"%d %lld %ld\n",metadata_rank++,offset+1,no_rec); // print metadata rank and offset in metadata file

			}

		}
j++;
	}while(fscanf(stud,"%lld %s %s %s %d",&id,name,branch,subject,&marks)!=EOF);
fclose(meta);
fclose(stud);
printf("function exit\n");
}

