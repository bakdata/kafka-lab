package com.bakdata.uni.range.extractor.type;

import com.bakdata.uni.range.padder.IntPadder;
import com.bakdata.uni.range.padder.LongPadder;
import com.bakdata.uni.range.padder.ZeroPadder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum FieldType {
    INTEGER(new IntPadder()),
    LONG(new LongPadder());
    @Getter
    private final ZeroPadder<?> zeroPadder;

}
