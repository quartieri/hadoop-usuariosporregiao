package br.com.hadoop.leo.usuariosporregiao.constantes;

import org.apache.hadoop.io.IntWritable;

public class Constantes {
	
	public static final String DELIMITADOR_DEFAULT = " ";
	public static final String DELIMITADOR_COMA = ";";
	public static final String DELIMITADOR_VIRGULA = ",";	
	public static final int COMPRIMENTO_DA_LINHA = 12;
	public final static IntWritable UM_WRITABLE = new IntWritable(1);
	public static final String JOB_NAME = "Usuario por Regiao JOB";
	public static final String JOB_NOT_SUCCESSFUL = "Ops... algum erro ocorreu durante a execucao do Job...";
	public static final String JOB_SUCCESSFUL = "Job executado com sucesso!";

}
