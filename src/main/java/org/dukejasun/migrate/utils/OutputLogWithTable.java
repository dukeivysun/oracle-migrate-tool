package org.dukejasun.migrate.utils;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.common.Constants;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class OutputLogWithTable {
    /**
     * 设置是否使用全角模式
     */
    @Setter
    private boolean sbcMode = true;
    private final String tableName;
    private List<Integer> columnCharCount;
    /**
     * 表头
     */
    private final List<List<String>> headerList = Lists.newArrayList();
    /**
     * 表格内容
     */
    private final List<List<String>> bodyList = Lists.newArrayList();
    /**
     * 表尾
     */
    private final List<List<String>> tailList = Lists.newArrayList();

    public OutputLogWithTable(String tableName) {
        this.tableName = tableName;
    }

    @Contract("_ -> new")
    public static @NotNull OutputLogWithTable create(String tableName) {
        return new OutputLogWithTable(tableName);
    }

    /**
     * 添加表头
     *
     * @param titles 列名
     */
    public void addHeader(String... titles) {
        if (CollectionUtils.isEmpty(columnCharCount)) {
            columnCharCount = new ArrayList<>(Collections.nCopies(titles.length, 0));
        }
        List<String> headers = new ArrayList<>();
        fillColumns(headers, titles);
        headerList.add(headers);
    }

    /**
     * 添加表体
     *
     * @param values 列值
     */
    public void addBody(String... values) {
        List<String> bodies = new ArrayList<>();
        bodyList.add(bodies);
        fillColumns(bodies, values);
    }

    /**
     * 添加表尾
     *
     * @param titles 标题
     */
    public void addTail(String... titles) {
        List<String> tails = new ArrayList<>();
        fillColumns(tails, titles);
        tailList.add(tails);
    }

    /**
     * 填充表头或者表体
     *
     * @param columns 被填充列表
     * @param values  填充值
     */
    private void fillColumns(List<String> columns, String @NotNull [] values) {
        for (int i = 0; i < values.length; i++) {
            String column = values[i];
            if (sbcMode) {
                column = toSbc(column);
            }
            columns.add(column);
            int width = column.length();
            if (!sbcMode) {
                int sbcCount = nonSbcCount(column);
                width = (width - sbcCount) * 2 + sbcCount;
            }
            if (width > columnCharCount.get(i)) {
                columnCharCount.set(i, width);
            }
        }
    }

    /**
     * 获取表格字符串
     */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        fillBorder(stringBuilder);
        if (!CollectionUtils.isEmpty(headerList)) {
            fillRows(stringBuilder, headerList);
            fillBorder(stringBuilder);
        }
        if (!CollectionUtils.isEmpty(bodyList)) {
            fillRows(stringBuilder, bodyList);
            fillBorder(stringBuilder);
        }
        if (!CollectionUtils.isEmpty(tailList)) {
            fillRows(stringBuilder, tailList);
            fillBorder(stringBuilder);
        }
        return stringBuilder.toString();
    }

    /**
     * 填充表头或者表体信息（多行）
     *
     * @param sb   内容
     * @param list 表头列表或者表体列表
     */
    private void fillRows(StringBuilder sb, @NotNull List<List<String>> list) {
        for (List<String> row : list) {
            sb.append(Constants.COLUMN_LINE);
            fillRow(sb, row);
            sb.append(Constants.LF);
        }
    }

    /**
     * 填充一行数据
     *
     * @param stringBuilder 内容
     * @param row           一行数据
     */
    private void fillRow(StringBuilder stringBuilder, @NotNull List<String> row) {
        final int size = row.size();
        String value;
        for (int i = 0; i < size; i++) {
            value = row.get(i);
            stringBuilder.append(sbcMode ? Constants.FULL_SPACE : Constants.HALF_SPACE);
            stringBuilder.append(value);
            final int length = value.length();
            final int sbcCount = nonSbcCount(value);
            if (sbcMode && sbcCount % 2 == 1) {
                stringBuilder.append(Constants.SPACE);
            }
            stringBuilder.append(sbcMode ? Constants.FULL_SPACE : Constants.HALF_SPACE);
            int maxLength = columnCharCount.get(i);
            int doubleNum = 2;
            if (sbcMode) {
                for (int j = 0; j < (maxLength - length + (sbcCount / doubleNum)); j++) {
                    stringBuilder.append(Constants.FULL_SPACE);
                }
            } else {
                for (int j = 0; j < (maxLength - ((length - sbcCount) * doubleNum + sbcCount)); j++) {
                    stringBuilder.append(Constants.HALF_SPACE);
                }
            }
            stringBuilder.append(Constants.COLUMN_LINE);
        }
    }

    /**
     * 填充边框
     *
     * @param stringBuilder StringBuilder
     */
    private void fillBorder(@NotNull StringBuilder stringBuilder) {
        stringBuilder.append(Constants.CORNER);
        for (Integer width : columnCharCount) {
            stringBuilder.append(sbcMode ? repeat(Constants.FULL_ROW_LINE, width + 2) : repeat(Constants.HALF_ROW_LINE, width + 2));
            stringBuilder.append(Constants.CORNER);
        }
        stringBuilder.append(Constants.LF);
    }

    /**
     * 打印到控制台
     */
    public void printToConsole() {
        log.info("{}\n{}", tableName, this);
    }

    /**
     * 半角字符数量<br/>
     * 英文字母、数字键、符号键
     *
     * @param value 字符串
     */
    private int nonSbcCount(@NotNull String value) {
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < '\177') {
                count++;
            }
        }
        return count;
    }

    /**
     * 重复字符
     *
     * @param c     字符
     * @param count 重复次数
     */
    private static String repeat(char c, int count) {
        if (count <= 0) {
            return Constants.EMPTY;
        }
        char[] result = new char[count];
        Arrays.fill(result, c);
        return new String(result);
    }

    /**
     * 转成全角字符
     *
     * @param input 字符
     */
    @Contract("_ -> new")
    private static @NotNull String toSbc(@NotNull String input) {
        final char[] c = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == ' ') {
                c[i] = '\u3000';
            } else if (c[i] < '\177') {
                c[i] = (char) (c[i] + 65248);

            }
        }
        return new String(c);
    }
}

