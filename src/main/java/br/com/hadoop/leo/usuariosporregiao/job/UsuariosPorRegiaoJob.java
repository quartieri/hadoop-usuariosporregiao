package br.com.hadoop.leo.usuariosporregiao.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import br.com.hadoop.leo.usuariosporregiao.constantes.Constantes;
import br.com.hadoop.leo.usuariosporregiao.job.mappers.UsuariosPorRegiaoMapper;
import br.com.hadoop.leo.usuariosporregiao.job.reducers.UsuariosPorRegiaoReducer;

/**
 * Exemplo simples de word count
 * Classe Driver, que monta o hadoop job unindo o nossos Mapper e o Reducer...
 * 
 * @author quartieri
 */
public class UsuariosPorRegiaoJob extends Configured implements Tool{
	final static Logger logger = Logger.getLogger(UsuariosPorRegiaoJob.class);

	/**
	 * Metodo main, chama o metodo run() do org.apache.hadoop.util.ToolRunner rodando nosso job com
	 * os argumentos de entrada
	 */
	public static void main(String[] args) throws Exception{
		logger.info("Iniciando metodo main...");
		int exitCode = ToolRunner.run(new UsuariosPorRegiaoJob(), args);
		System.exit(exitCode);
	}
 
	/**
	 * Metodo Run, esquedula nosso o Hadoop job
	 * @param args - vindo da entrada do programa
	 */
	public int run(String[] args) throws Exception {
		logger.info("Metodo run...");
		if (args.length != 2) {
			System.err.printf(
					"Dica: %s precisa de 2 argumetos <entrada_dados> <saida_dados>\n",
					getClass().getSimpleName()
					);
			return -1;
		}
	  

		//Inicializa e configura o Hadoop job 
		Job job = new Job();
		job.setJarByClass(UsuariosPorRegiaoJob.class);
		logger.info("JOB_NAME = "+Constantes.JOB_NAME);
		job.setJobName(Constantes.JOB_NAME); 
		    
		//Configura a entrada e saida de dados do nosso Hadoop Job...
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		// como o dado saira...
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Seta nosso mapper e nosso reducer para o job
		job.setMapperClass(UsuariosPorRegiaoMapper.class);
		job.setReducerClass(UsuariosPorRegiaoReducer.class);
	
		//Solicita execucao do job, colhendo o resultado do mesmo
		int valorRetornado = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			logger.info(Constantes.JOB_SUCCESSFUL);
		} else if(!job.isSuccessful()) {
			logger.info(Constantes.JOB_NOT_SUCCESSFUL);			
		}
		
		return valorRetornado;
	}
}
