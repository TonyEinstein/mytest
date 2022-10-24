package com.mytest.employee;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

/**
 * Created by ua07 on 9/24/19.
 */
public class SalaryEmployee implements Writable {

    private int empno;
    private String ename;
    private String job;
    private int mgr;
    private String hiredate;
    private int sal;
    private int comm;
    private int deptno;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.empno);
        dataOutput.writeUTF(this.ename);
        dataOutput.writeUTF(this.job);
        dataOutput.writeInt(this.mgr);
        dataOutput.writeUTF(this.hiredate);
        dataOutput.writeInt(this.sal);
        dataOutput.writeInt(this.comm);
        dataOutput.writeInt(this.deptno);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.empno = dataInput.readInt();
        this.ename = dataInput.readUTF();
        this.job = dataInput.readUTF();
        this.mgr = dataInput.readInt();
        this.hiredate = dataInput.readUTF();
        this.sal = dataInput.readInt();
        this.comm = dataInput.readInt();
        this.deptno = dataInput.readInt();
    }

    public int getEmpno(){
        return empno;
    }
    public void setEmpno(int empno){
        this.empno = empno;
    }

    public String getEname(){
        return ename;
    }
    public void setEname(String word){
        this.ename = ename;
    }

    public String getJob(){
        return job;
    }
    public void setJob(String job){
        this.job = job;
    }

    public int getMgr(){
        return mgr;
    }
    public void setMgr(int mgr){
        this.mgr = mgr;
    }

    public String getHiredate(){
        return hiredate;
    }
    public void setHiredate(String hiredate){
        this.hiredate = hiredate;
    }

    public int getSal(){
        return sal;
    }
    public void setSal(int sal){
        this.sal = sal;
    }

    public int getComm(){
        return comm;
    }
    public void setComm(int comm){
        this.comm = comm;
    }

    public int getDeptno(){
        return deptno;
    }
    public void setDeptno(int deptno){
        this.deptno = deptno;
    }


}
