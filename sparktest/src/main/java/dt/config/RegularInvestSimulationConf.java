package dt.config;

import java.math.BigDecimal;
import java.util.List;

public class RegularInvestSimulationConf {

    private List<RegularInvestSimulationTask> tasks;

    public List<RegularInvestSimulationTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<RegularInvestSimulationTask> tasks) {
        this.tasks = tasks;
    }

    static public class RegularInvestSimulationTask {

        private BigDecimal apply;

        private String regularInvestType;

        private String strategyType;

        private int stockAvgDay;


        public BigDecimal getApply() {
            return apply;
        }

        public void setApply(BigDecimal apply) {
            this.apply = apply;
        }

        public String getRegularInvestType() {
            return regularInvestType;
        }

        public void setRegularInvestType(String regularInvestType) {
            this.regularInvestType = regularInvestType;
        }

        public String getStrategyType() {
            return strategyType;
        }

        public void setStrategyType(String strategyType) {
            this.strategyType = strategyType;
        }

        public int getStockAvgDay() {
            return stockAvgDay;
        }

        public void setStockAvgDay(int stockAvgDay) {
            this.stockAvgDay = stockAvgDay;
        }
    }
}
