package com.intentmedia.admm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.intentmedia.admm.AdmmIterationHelper.admmReducerContextToJson;
import static com.intentmedia.admm.AdmmIterationHelper.jsonToAdmmReducerContext;

public class AdmmReducerContext implements Writable {

    @JsonProperty("uInitial")
    private double[] uInitial;

    @JsonProperty("xInitial")
    private double[] xInitial;

    @JsonProperty("xUpdated")
    private double[] xUpdated;

    @JsonProperty("zInitial")
    private double[] zInitial; // zInitial is only needed here for calculating the dual norm, used in the rho update

    @JsonProperty("primalObjectiveValue")
    private double primalObjectiveValue;

    @JsonProperty("rho")
    private double rho;

    @JsonProperty("lambdaValue")
    private double lambdaValue;

    @JsonProperty("mapStartTime")
    private long mapStartTime;

    @JsonProperty("optimizationStartTime")
    private long optimizationStartTime;

    @JsonProperty("mapEndTime")
    private long mapEndTime;

    public AdmmReducerContext(double[] uInitial, double[] xInitial, double[] xUpdated, double[] zInitial,
                              double primalObjectiveValue, double rho, double lambdaValue) {
        this.uInitial = uInitial;
        this.xInitial = xInitial;
        this.xUpdated = xUpdated;
        this.zInitial = zInitial;
        this.primalObjectiveValue = primalObjectiveValue;
        this.rho = rho;
        this.lambdaValue = lambdaValue;
    }

    public AdmmReducerContext(double[] uInitial, double[] xInitial, double[] xUpdated, double[] zInitial,
                              double primalObjectiveValue, double rho, double lambdaValue,
                              long mapStartTime, long optimizationStartTime, long mapEndTime) {
        this.uInitial = uInitial;
        this.xInitial = xInitial;
        this.xUpdated = xUpdated;
        this.zInitial = zInitial;
        this.primalObjectiveValue = primalObjectiveValue;
        this.rho = rho;
        this.lambdaValue = lambdaValue;
        this.mapStartTime = mapStartTime;
        this.optimizationStartTime = optimizationStartTime;
        this.mapEndTime = mapEndTime;
    }

    public AdmmReducerContext() {
    }

    public void setAdmmReducerContext(AdmmReducerContext context) {
        this.uInitial = context.uInitial;
        this.xInitial = context.xInitial;
        this.xUpdated = context.xUpdated;
        this.zInitial = context.zInitial;
        this.primalObjectiveValue = context.primalObjectiveValue;
        this.rho = context.rho;
        this.lambdaValue = context.lambdaValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text contextJson = new Text(admmReducerContextToJson(this));
        contextJson.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Text contextJson = new Text();
        contextJson.readFields(in);
        setAdmmReducerContext(jsonToAdmmReducerContext(contextJson.toString()));
    }

    @JsonProperty("uInitial")
    public double[] getUInitial() {
        return uInitial;
    }

    @JsonProperty("xInitial")
    public double[] getXInitial() {
        return xInitial;
    }

    @JsonProperty("xUpdated")
    public double[] getXUpdated() {
        return xUpdated;
    }

    @JsonProperty("zInitial")
    public double[] getZInitial() {
        return zInitial;
    }

    @JsonProperty("primalObjectiveValue")
    public double getPrimalObjectiveValue() {
        return primalObjectiveValue;
    }

    @JsonProperty("rho")
    public double getRho() {
        return rho;
    }

    @JsonProperty("lambdaValue")
    public double getLambdaValue() {
        return lambdaValue;
    }

    @JsonProperty("mapStartTime")
    public long getMapStartTime() {
        return mapStartTime;
    }

    @JsonProperty("optimizationStartTime")
    public long getOptimizationStartTime() {
        return optimizationStartTime;
    }

    @JsonProperty("mapEndTime")
    public long getMapEndTime() {
        return mapEndTime;
    }
}
